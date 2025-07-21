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
            duration_secs    REAL    NOT NULL
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
        cur.close()
        conn.close()

    def search(self, keyword: str, channel_id: str | None = None) -> List[SegmentInfo]:
        """
        Açar sözü böyük/kiçik hərf fərqinə baxmadan axtarır.
        Əgər channel_id verilsə, yalnız o kanalda axtarış edir.
        Nəticələri start_time üzrə sıralayır.
        """
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
                 ORDER BY start_time
            """, (ilike_kw, channel_id))
        else:
            cur.execute("""
                SELECT channel_id, start_time, end_time, text,
                       segment_filename, offset_secs, duration_secs
                  FROM transcripts
                 WHERE text ILIKE %s
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
                 ORDER BY start_time
            """, (start_time, end_time, channel_id))
        else:
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
