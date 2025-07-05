# app/services/db.py

import psycopg2
from typing import List
from app.api.schemas import SegmentInfo

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
        """
        Ensure the transcripts table exists.
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transcripts (
            id               SERIAL PRIMARY KEY,
            start_time       TIMESTAMP WITH TIME ZONE NOT NULL,
            end_time         TIMESTAMP WITH TIME ZONE NOT NULL,
            text             TEXT NOT NULL,
            segment_filename TEXT NOT NULL,
            offset_secs      REAL NOT NULL,
            duration_secs    REAL NOT NULL
        )
        """)
        conn.commit()
        cur.close()
        conn.close()

    def insert_segments(self, segments: List[SegmentInfo]):
        """
        Insert a batch of whisper‐generated segments into the DB.
        """
        conn = self.get_conn()
        cur = conn.cursor()
        for seg in segments:
            cur.execute("""
                INSERT INTO transcripts
                  (start_time, end_time, text,
                   segment_filename, offset_secs, duration_secs)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                seg.start_time,
                seg.end_time,
                seg.text,
                seg.segment_filename,
                seg.offset_secs,
                seg.duration_secs
            ))
        conn.commit()
        cur.close()
        conn.close()

    def search(self, keyword: str) -> List[SegmentInfo]:
        """
        Return all segments containing keyword, ordered by start_time.
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT start_time, end_time, text,
                   segment_filename, offset_secs, duration_secs
              FROM transcripts
             WHERE text ILIKE %s
             ORDER BY start_time
        """, (f"%{keyword}%",))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [
            SegmentInfo(
                start_time       = r[0].isoformat(),
                end_time         = r[1].isoformat(),
                text             = r[2],
                segment_filename = r[3],
                offset_secs      = float(r[4]),
                duration_secs    = float(r[5])
            )
            for r in rows
        ]

    def fetch_text(self, start_time: str, end_time: str) -> str:
        """
        Return all 'text' in the given time window.
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT text
              FROM transcripts
             WHERE start_time >= %s
               AND end_time   <= %s
             ORDER BY start_time
        """, (start_time, end_time))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return " ".join(r[0] for r in rows)
