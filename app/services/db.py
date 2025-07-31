# app/services/db.py
#!/usr/bin/env python3
import psycopg2
from typing import List, NamedTuple
from app.api.schemas import SegmentInfo
import datetime
import logging

logger = logging.getLogger(__name__)


class OldSegment(NamedTuple):
    id: int
    channel_id: str
    segment_filename: str

class ScheduleInterval(NamedTuple):
    id: int
    start_time: datetime.time
    end_time:   datetime.time

class DBClient:
    def __init__(self, settings):
        self._conf = settings

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
        )
        """)
        conn.commit()
        cur.close()
        conn.close()

    def insert_segments(self, segments: List[SegmentInfo]):
        conn = self.get_conn()
        cur = conn.cursor()
        for seg in segments:
            cur.execute("""
                INSERT INTO transcripts
                  (channel_id, start_time, end_time, text,
                   segment_filename, offset_secs, duration_secs)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                seg.channel_id,
                seg.start_time,
                seg.end_time,
                seg.text,
                seg.segment_filename,
                seg.offset_secs,
                seg.duration_secs
            ))
        conn.commit()
        logger.info("%d seqment DB-yə yazıldı", len(segments))
        cur.close()
        conn.close()

    def search(self, keyword: str, channel_id: str | None = None) -> List[SegmentInfo]:
        conn = self.get_conn()
        cur = conn.cursor()
        ilike_kw = f"%{keyword}%"

        if channel_id:
            cur.execute("""
                SELECT channel_id, start_time, end_time, text,
                       segment_filename, offset_secs, duration_secs
                  FROM transcripts
                 WHERE text ILIKE %s
                   AND channel_id = %s
                   AND deleted = FALSE
                 ORDER BY start_time
            """, (ilike_kw, channel_id))
        else:
            cur.execute("""
                SELECT channel_id, start_time, end_time, text,
                       segment_filename, offset_secs, duration_secs
                  FROM transcripts
                 WHERE text ILIKE %s
                   AND deleted = FALSE
                 ORDER BY start_time
            """, (ilike_kw,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [
            SegmentInfo(
                channel_id       = r[0],
                start_time       = r[1].isoformat(),
                end_time         = r[2].isoformat(),
                text             = r[3],
                segment_filename = r[4],
                offset_secs      = float(r[5]),
                duration_secs    = float(r[6])
            )
            for r in rows
        ]

    def fetch_text(self, start_time: str, end_time: str, channel_id: str | None = None) -> str:
        conn = self.get_conn()
        cur = conn.cursor()

        if channel_id:
            cur.execute("""
                SELECT text
                  FROM transcripts
                 WHERE start_time >= %s
                   AND end_time   <= %s
                   AND channel_id = %s
                   AND deleted = FALSE
                 ORDER BY start_time
            """, (start_time, end_time, channel_id))
        else:
            cur.execute("""
                SELECT text
                  FROM transcripts
                 WHERE start_time >= %s
                   AND end_time   <= %s
                   AND deleted = FALSE
                 ORDER BY start_time
            """, (start_time, end_time))

        rows = cur.fetchall()
        cur.close()
        conn.close()
        return " ".join(r[0] for r in rows)

    # ——— Aşağıdakılar fayl təmizləmə üçün ———

    def get_segments_older_than(self, cutoff: datetime.datetime) -> List[OldSegment]:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, channel_id, segment_filename
              FROM transcripts
             WHERE end_time < %s
               AND deleted = FALSE
        """, (cutoff,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [OldSegment(*r) for r in rows]

    def mark_segments_deleted(self, ids: List[int]) -> None:
        if not ids:
            return
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE transcripts SET deleted = TRUE WHERE id = ANY(%s)",
            (ids,)
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
          )
        """)
        conn.commit()
        cur.close()
        conn.close()

    def get_intervals(self) -> list[ScheduleInterval]:
        conn = self.get_conn(); cur = conn.cursor()
        cur.execute("SELECT id, start_time, end_time FROM schedule_intervals ORDER BY id")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [ScheduleInterval(*r) for r in rows]

    def add_interval(self, start_time: datetime.time, end_time: datetime.time) -> ScheduleInterval:
        conn = self.get_conn(); cur = conn.cursor()
        cur.execute(
          "INSERT INTO schedule_intervals (start_time, end_time) VALUES (%s, %s) RETURNING id",
          (start_time, end_time)
        )
        new_id = cur.fetchone()[0]
        conn.commit(); cur.close(); conn.close()
        return ScheduleInterval(new_id, start_time, end_time)

    def update_interval(self, id: int, start_time: datetime.time, end_time: datetime.time) -> None:
        conn = self.get_conn(); cur = conn.cursor()
        cur.execute(
          "UPDATE schedule_intervals SET start_time=%s, end_time=%s WHERE id=%s",
          (start_time, end_time, id)
        )
        conn.commit(); cur.close(); conn.close()

    def delete_interval(self, id: int) -> None:
        conn = self.get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM schedule_intervals WHERE id=%s", (id,))
        conn.commit(); cur.close(); conn.close()
    