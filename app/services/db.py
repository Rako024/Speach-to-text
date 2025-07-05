# app/services/db.py

from typing import List
import psycopg2
from app.api.schemas import SegmentInfo

class DBClient:
    def __init__(self, settings):
        self._conf = settings
        # Biz hər çağırışda yeni bağlantı açırıq, alternativ olaraq
        # bir dəfə açıb saxlayıb da istifadə edə bilərsiniz.
    def get_conn(self):
        return psycopg2.connect(
            host=self._conf.db_host,
            port=self._conf.db_port,
            database=self._conf.db_name,
            user=self._conf.db_user,
            password=self._conf.db_password
        )

    # ... mövcud init_db və insert_segments metodları burda qalır ...

    def search(self, keyword: str) -> List[SegmentInfo]:
        conn = self.get_conn()
        cur  = conn.cursor()
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

        # Tapılmadısa FastAPI tərəfində 404 qaldırıla bilər
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
