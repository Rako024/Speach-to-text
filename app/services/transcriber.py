#!/usr/bin/env python3
import os
import datetime
from typing import List

from faster_whisper import WhisperModel
from app.api.schemas import SegmentInfo


class Transcriber:
    """
    WAV faylını Whisper vasitəsilə transkripsiya edən sinif.
    """

    def __init__(self, settings):
        # Whisper modelini yükle
        self.model = WhisperModel(
            settings.whisper_model,
            device=settings.device,
            compute_type=settings.compute_type
        )

    def transcribe(self, wav_path: str, start_ts: float) -> List[SegmentInfo]:
        """
        Verilmiş WAV yolunu transkripsiya edir və hər bir tapılmış
        seqment üçün SegmentInfo siyahısı qaytarır.

        :param wav_path: Lokal WAV faylının tam yolu
        :param start_ts: Seqmentin başladığı epoch ilə ifadə olunan zaman
        :return: List[SegmentInfo]
        """
        # Whisper transcribe çağırışı
        segments, _ = self.model.transcribe(
            wav_path,
            language="az",
            beam_size=4,
            best_of=4,
            vad_filter=True
        )

        result: List[SegmentInfo] = []
        for seg in segments:
            # Absolyut başlanğıc və son zamanlarını hesabla
            abs_start_ts = start_ts + seg.start
            abs_end_ts   = start_ts + seg.end

            abs_start = datetime.datetime.fromtimestamp(
                abs_start_ts, datetime.timezone.utc
            )
            abs_end = datetime.datetime.fromtimestamp(
                abs_end_ts, datetime.timezone.utc
            )

            # .wav fayl adından TS fayl adını çıxar
            # misal: "segment_012.wav" → idx=12 → "segment_00012.ts"
            basename = os.path.basename(wav_path)
            idx = int(basename.split('_')[1].split('.')[0])
            ts_file = f"segment_{idx:05d}.ts"

            # SegmentInfo obyektini doldur
            result.append(SegmentInfo(
                start_time       = abs_start.isoformat(),
                end_time         = abs_end.isoformat(),
                text             = seg.text.strip(),
                segment_filename = ts_file,
                offset_secs      = float(seg.start),
                duration_secs    = float(seg.end - seg.start)
            ))

        return result
