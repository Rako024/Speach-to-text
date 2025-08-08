#!/usr/bin/env python3
from __future__ import annotations

import logging
from typing import List, Optional, NamedTuple, Dict, Tuple
from datetime import date, datetime, time

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class ScheduleInterval(NamedTuple):
    id: int
    start_time: time
    end_time: time

class TranscriptGC(NamedTuple):
    id: int
    channel_id: str
    segment_filename: str

class DBClient:
    def __init__(self, settings):
        self._conf = settings

        # sslmode (Neon üçün tələb oluna bilər)
        sslmode = getattr(self._conf, "db_sslmode", None)
        sslchunk = f" sslmode={sslmode}" if sslmode else ""

        # DSN
        self.dsn = (
            f"host={self._conf.db_host} port={self._conf.db_port}"
            f" dbname={self._conf.db_name} user={self._conf.db_user}"
            f" password={self._conf.db_password}"
            f"{sslchunk}"
        )

        # Pool parametrləri (default: 1..10)
        minconn = int(getattr(self._conf, "db_pool_min", 1))
        maxconn = int(getattr(self._conf, "db_pool_max", 10))
        if maxconn < minconn:
            maxconn = minconn

        # Connection Pool
        self.pool: pg_pool.SimpleConnectionPool = pg_pool.SimpleConnectionPool(
            minconn, maxconn, dsn=self.dsn
        )
        logger.info("DB connection pool created (min=%d, max=%d)", minconn, maxconn)

    # ----- Low-level helpers -----
    def get_conn(self):
        """Compat: Pool-dan connection qaytarır. İstifadədən sonra putconn edilməlidir."""
        return self.pool.getconn()

    def put_conn(self, conn) -> None:
        """Compat: Connection-u pool-a qaytar."""
        try:
            self.pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    # Context manager (commit/rollback + putconn)
    def _cursor(self, dict_cursor: bool = False):
        class _Ctx:
            def __init__(self, outer: DBClient, dict_cursor: bool):
                self.outer = outer
                self.dict_cursor = dict_cursor
                self.conn = None
                self.cur = None
            def __enter__(self) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
                self.conn = self.outer.pool.getconn()
                if self.dict_cursor:
                    self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
                else:
                    self.cur = self.conn.cursor()
                return self.conn, self.cur
            def __exit__(self, exc_type, exc, tb):
                try:
                    if exc_type is None:
                        self.conn.commit()
                    else:
                        self.conn.rollback()
                finally:
                    try:
                        self.cur.close()
                    except Exception:
                        pass
                    self.outer.put_conn(self.conn)
        return _Ctx(self, dict_cursor)

    # ----- Schema init -----
    def init_db(self):
        with self._cursor() as (conn, cur):
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS transcripts (
                id               SERIAL PRIMARY KEY,
                channel_id       TEXT         NOT NULL,
                start_time       TIMESTAMPTZ  NOT NULL,
                end_time         TIMESTAMPTZ  NOT NULL,
                text             TEXT         NOT NULL,
                segment_filename TEXT         NOT NULL,
                offset_secs      REAL         NOT NULL,
                duration_secs    REAL         NOT NULL,
                deleted          BOOLEAN      NOT NULL DEFAULT FALSE
            );
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_transcripts_text_trgm "
                "ON transcripts USING GIN (text gin_trgm_ops);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_transcripts_start_time "
                "ON transcripts (start_time);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_transcripts_channel_id "
                "ON transcripts (channel_id);"
            )
            # GC + overlap sorğuları üçün əlavə indekslər
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_transcripts_deleted_end_time "
                "ON transcripts (deleted, end_time);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_transcripts_channel_time "
                "ON transcripts (channel_id, start_time, end_time);"
            )
        logger.info("DB init done")

    def init_schedule_table(self):
        with self._cursor() as (conn, cur):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schedule_intervals (
                    id         SERIAL PRIMARY KEY,
                    start_time TIME    NOT NULL,
                    end_time   TIME    NOT NULL
                );
            """)
        logger.info("Schedule table ensured")

    # ----- Write path -----
    def insert_segments(self, segments: List[dict]) -> None:
        if not segments:
            return
        with self._cursor() as (conn, cur):
            for seg in segments:
                cur.execute(
                    """
                    INSERT INTO transcripts
                      (channel_id, start_time, end_time, text,
                       segment_filename, offset_secs, duration_secs)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        seg['channel_id'],
                        seg['start_time'],
                        seg['end_time'],
                        seg['text'],
                        seg['segment_filename'],
                        seg['offset_secs'],
                        seg['duration_secs'],
                    )
                )
        logger.info("%d segments inserted into DB", len(segments))

    # ----- Read path -----
    def search(
        self,
        keyword: str,
        *,
        channel: Optional[str]    = None,
        start_date: Optional[date]= None,
        end_date:   Optional[date]= None,
        threshold:  float         = 0.2,
        limit:      int           = 50
    ) -> List[dict]:
        with self._cursor(dict_cursor=True) as (conn, cur):
            clauses = ["(text ILIKE %(kw_sub)s OR similarity(text, %(kw)s) > %(thr)s)"]
            params = {
                'kw':     keyword,
                'kw_sub': f"%{keyword}%",
                'thr':    threshold,
                'lim':    limit
            }
            if channel:
                clauses.append("channel_id = %(channel)s")
                params['channel'] = channel
            if start_date:
                clauses.append("start_time::date >= %(start_date)s")
                params['start_date'] = start_date
            if end_date:
                clauses.append("end_time::date <= %(end_date)s")
                params['end_date'] = end_date
            clauses.append("deleted = FALSE")

            where_sql = ' AND '.join(clauses)
            sql = f"""
            SELECT
              id,
              channel_id,
              start_time,
              end_time,
              text,
              segment_filename,
              offset_secs,
              duration_secs,
              GREATEST(
                (CASE WHEN text ILIKE %(kw_sub)s THEN 1.0 ELSE 0 END),
                similarity(text, %(kw)s)
              ) AS score
            FROM transcripts
            WHERE {where_sql}
            ORDER BY score DESC, start_time ASC
            LIMIT %(lim)s;
            """
            cur.execute(sql, params)
            results = cur.fetchall()
            return results

    def fetch_text(
        self,
        start_time: datetime,
        end_time:   datetime,
        channel: Optional[str] = None
    ) -> str:
        clauses = ["start_time >= %s", "end_time <= %s", "deleted = FALSE"]
        params = [start_time, end_time]
        if channel:
            clauses.append("channel_id = %s")
            params.append(channel)
        where_sql = ' AND '.join(clauses)

        with self._cursor() as (conn, cur):
            cur.execute(
                f"SELECT text FROM transcripts WHERE {where_sql} ORDER BY start_time;",
                params
            )
            texts = [r[0] for r in cur.fetchall()]
        return " ".join(texts)

    def get_intervals(self) -> List[ScheduleInterval]:
        with self._cursor() as (conn, cur):
            cur.execute("SELECT id, start_time, end_time FROM schedule_intervals ORDER BY id;")
            rows = cur.fetchall()
        return [ScheduleInterval(*r) for r in rows]

    def add_interval(self, start_time: time, end_time: time) -> ScheduleInterval:
        with self._cursor() as (conn, cur):
            cur.execute(
                "INSERT INTO schedule_intervals (start_time, end_time) VALUES (%s, %s) RETURNING id;",
                (start_time, end_time)
            )
            new_id = cur.fetchone()[0]
        return ScheduleInterval(new_id, start_time, end_time)

    def update_interval(self, id: int, start_time: time, end_time: time) -> None:
        with self._cursor() as (conn, cur):
            cur.execute(
                "UPDATE schedule_intervals SET start_time=%s, end_time=%s WHERE id=%s;",
                (start_time, end_time, id)
            )

    def delete_interval(self, id: int) -> None:
        with self._cursor() as (conn, cur):
            cur.execute("DELETE FROM schedule_intervals WHERE id=%s;", (id,))

    # ---- Yeni metodlar ----
    def get_segment(self, segment_id: int) -> Dict:
        with self._cursor(dict_cursor=True) as (conn, cur):
            cur.execute("""
                SELECT
                  id, channel_id, start_time, end_time,
                  text, segment_filename, offset_secs, duration_secs
                FROM transcripts
                WHERE id = %s AND deleted = FALSE
            """, (segment_id,))
            row = cur.fetchone()
            return row

    def fetch_segments_in_window(
        self,
        channel: str,
        start_iso: str,
        end_iso:   str
    ) -> List[Dict]:
        # Overlap: start_time < window_end AND end_time > window_start
        with self._cursor(dict_cursor=True) as (conn, cur):
            cur.execute("""
                SELECT
                  id, channel_id, start_time, end_time,
                  text, segment_filename, offset_secs, duration_secs
                FROM transcripts
                WHERE channel_id = %s
                  AND start_time < %s   -- window_end
                  AND end_time   > %s   -- window_start
                  AND deleted = FALSE
                ORDER BY start_time;
            """, (channel, end_iso, start_iso))
            rows = cur.fetchall()
            return rows

    def get_segments_older_than(self, cutoff: datetime) -> List[TranscriptGC]:
        with self._cursor() as (conn, cur):
            cur.execute("""
                SELECT id, channel_id, segment_filename
                FROM transcripts
                WHERE end_time < %s AND deleted = FALSE
                ORDER BY end_time
            """, (cutoff,))
            rows = cur.fetchall()
        return [TranscriptGC(*r) for r in rows]

    def mark_segments_deleted(self, ids: List[int]) -> None:
        if not ids:
            return
        with self._cursor() as (conn, cur):
            cur.execute(
                "UPDATE transcripts SET deleted = TRUE WHERE id = ANY(%s);",
                (ids,)
            )

    # ----- Shutdown helper -----
    def close(self):
        try:
            self.pool.closeall()
            logger.info("DB connection pool closed")
        except Exception:
            pass
