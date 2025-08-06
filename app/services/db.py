#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional, NamedTuple, Dict
from datetime import date, datetime, time
import logging

logger = logging.getLogger(__name__)

class ScheduleInterval(NamedTuple):
    id: int
    start_time: time
    end_time: time

class DBClient:
    def __init__(self, settings):
        self._conf = settings
        # connection string üçün DSN
        self.dsn = (
            f"host={self._conf.db_host} port={self._conf.db_port}"
            f" dbname={self._conf.db_name} user={self._conf.db_user}"
            f" password={self._conf.db_password}"
        )

    def get_conn(self):
        return psycopg2.connect(
            host=self._conf.db_host,
            port=self._conf.db_port,
            database=self._conf.db_name,
            user=self._conf.db_user,
            password=self._conf.db_password
        )

    def init_db(self):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transcripts (
            id               SERIAL PRIMARY KEY,
            channel_id       TEXT    NOT NULL,
            start_time       TIMESTAMPTZ NOT NULL,
            end_time         TIMESTAMPTZ NOT NULL,
            text             TEXT    NOT NULL,
            segment_filename TEXT    NOT NULL,
            offset_secs      REAL    NOT NULL,
            duration_secs    REAL    NOT NULL,
            deleted          BOOLEAN NOT NULL DEFAULT FALSE
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
        conn.commit()
        cur.close()
        conn.close()

    def init_schedule_table(self):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schedule_intervals (
                id         SERIAL PRIMARY KEY,
                start_time TIME    NOT NULL,
                end_time   TIME    NOT NULL
            );
        """)
        conn.commit()
        cur.close()
        conn.close()

    def insert_segments(self, segments: List[dict]) -> None:
        conn = self.get_conn()
        cur = conn.cursor()
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
        conn.commit()
        logger.info("%d segments inserted into DB", len(segments))
        cur.close()
        conn.close()

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
        conn = psycopg2.connect(self.dsn, cursor_factory=RealDictCursor)
        cur = conn.cursor()

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
        cur.close()
        conn.close()
        return results

    def fetch_text(
        self,
        start_time: datetime,
        end_time:   datetime,
        channel: Optional[str] = None
    ) -> str:
        conn = self.get_conn()
        cur = conn.cursor()
        clauses = ["start_time >= %s", "end_time <= %s", "deleted = FALSE"]
        params = [start_time, end_time]
        if channel:
            clauses.append("channel_id = %s")
            params.append(channel)
        where_sql = ' AND '.join(clauses)

        cur.execute(
            f"SELECT text FROM transcripts WHERE {where_sql} ORDER BY start_time;",
            params
        )
        texts = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return " ".join(texts)

    def get_intervals(self) -> List[ScheduleInterval]:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, start_time, end_time FROM schedule_intervals ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [ScheduleInterval(*r) for r in rows]

    def add_interval(self, start_time: time, end_time: time) -> ScheduleInterval:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO schedule_intervals (start_time, end_time) VALUES (%s, %s) RETURNING id;",
            (start_time, end_time)
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return ScheduleInterval(new_id, start_time, end_time)

    def update_interval(self, id: int, start_time: time, end_time: time) -> None:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE schedule_intervals SET start_time=%s, end_time=%s WHERE id=%s;",
            (start_time, end_time, id)
        )
        conn.commit()
        cur.close()
        conn.close()

    def delete_interval(self, id: int) -> None:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM schedule_intervals WHERE id=%s;", (id,))
        conn.commit()
        cur.close()
        conn.close()

    # ——————————————————————————
    # Yeni metodlar:
    # ——————————————————————————

    def get_segment(self, segment_id: int) -> Dict:
        """
        Verilmiş id-li seqmenti transcripts cədvəlindən götürür.
        """
        conn = self.get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT
              id, channel_id, start_time, end_time,
              text, segment_filename, offset_secs, duration_secs
            FROM transcripts
            WHERE id = %s AND deleted = FALSE
        """, (segment_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row

    def fetch_segments_in_window(
        self,
        channel: str,
        start_iso: str,
        end_iso:   str
    ) -> List[Dict]:
        """
        Verilmiş kanal və ISO vaxt aralığındakı seqmentləri qaytarır.
        """
        conn = self.get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT
              id, channel_id, start_time, end_time,
              text, segment_filename, offset_secs, duration_secs
            FROM transcripts
            WHERE channel_id = %s
              AND start_time >= %s
              AND end_time   <= %s
              AND deleted = FALSE
            ORDER BY start_time;
        """, (channel, start_iso, end_iso))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
