#!/usr/bin/env python3
import os
import datetime
from typing import List, Dict

from faster_whisper import WhisperModel

class Transcriber:
    """
    WAV faylını Whisper vasitəsilə transkripsiya edən sinif.
    İndi raw dict siyahısı qaytarır, Pydantic modelləşdirməni main.py-də edəcəyik.
    """

    def __init__(self, settings):
        # Whisper modelini yükləyirik
        self.model = WhisperModel(
            settings.whisper_model,
            device=settings.device,
            compute_type=settings.compute_type
        )

    def transcribe(self, wav_path: str, start_ts: float) -> List[Dict]:
        """
        Verilmiş WAV faylını transkripsiya edir və hər bir seqment üçün dict siyahısı qaytarır.

        :param wav_path: Lokal WAV faylının tam yolu
        :param start_ts:  Seqmentin başladığı epoch şəklində zaman
        :return:          List[Dict] — hər dict Pydantic SegmentInfo-un girişinə uyğun
        """
        # Whisper transcribe çağırışı
        segments, _ = self.model.transcribe(
            wav_path,
            language="az",
            beam_size=4,
            best_of=4,
            vad_filter=False
        )

        # .wav fayl adından eyni baza ilə .ts adını çıxarırıq:
        # misal: "itv_20250721T143236.wav" → "itv_20250721T143236.ts"
        basename         = os.path.basename(wav_path)
        name_without_ext, _ = os.path.splitext(basename)
        ts_file          = f"{name_without_ext}.ts"

        result: List[Dict] = []
        for seg in segments:
            # Absolyut vaxtları hesabla
            abs_start = datetime.datetime.fromtimestamp(
                start_ts + seg.start, datetime.timezone.utc
            )
            abs_end   = datetime.datetime.fromtimestamp(
                start_ts + seg.end,   datetime.timezone.utc
            )

            result.append({
                "start_time":       abs_start.isoformat(),
                "end_time":         abs_end.isoformat(),
                "text":             seg.text.strip(),
                "segment_filename": ts_file,
                "offset_secs":      float(seg.start),
                "duration_secs":    float(seg.end - seg.start)
            })

        return result
