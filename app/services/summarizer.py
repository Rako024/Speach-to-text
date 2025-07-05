# app/services/deepseek_client.py

import requests
import logging
from typing import List, Optional
from app.config import Settings
from app.api.schemas import SegmentInfo

logger = logging.getLogger(__name__)

class DeepSeekClient:
    """
    DeepSeek API ilə əlaqə saxlayır, həm tam transkriptləri, həm də açar sözə fokuslanmış xülasələri hazırlayır.
    """

    def __init__(self, settings: Settings):
        self.api_url = settings.deepseek_api_url
        self.api_key = settings.deepseek_key

    def summarize(
        self,
        segments: List[SegmentInfo],
        keyword: Optional[str] = None
    ) -> str:
        """
        Açar sözlə axtarış nəticəsində əldə olunmuş SegmentInfo-ları birləşdirib xülasə verir.
        Əgər `keyword` verilibsə, o söz ətrafında 2–3 cümləlik fokuslanmış xülasə,
        yoxdursa ümumi nöqtəli bəndli xülasə qaytarır.
        """
        full_text = " ".join(seg.text for seg in segments)

        system_prompt = (
            "Sən transkript mətinlərini xülasə etmək üçün ixtisaslaşmış modelisən. "
            "Cavabını Azərbaycan dilində, aydın və konkret ver."
            "Tam olaraq sənə göndərilən mətində nə danışıldığını, nədən bəhsolunuduğu bizə açıqla. əlavə uzadıcı ifadə bildirici sözlər yazma."
            "Ən sonda isə sintaktik və məna səhvlərinin düzəldilmiş versiyadakı mətini - Verimiş mətn : - deyərək sonda yaz."
        )
        if keyword:
            user_prompt = (
                f"Verilmiş mətndə “{keyword}” sözü ilə bağlı bütün cümlələri "
                f"birinəşdirərək 2–3 cümləlik xülasə hazırla:\n\n{full_text}"
            )
        else:
            user_prompt = (
                f"Aşağıdakı transkripti oxu və əsas məqamları qısa, nöqtəli bəndlərlə ver:\n\n{full_text}"
            )

        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt}
            ],
            "max_tokens": 1024,
            "temperature": 0.3,
            "stream": False
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        resp = requests.post(self.api_url, headers=headers, json=payload)
        if resp.status_code != 200:
            logger.error("DeepSeek API error %s: %s", resp.status_code, resp.text)
            raise RuntimeError("DeepSeek API error")

        return resp.json()["choices"][0]["message"]["content"]

    def summarize_text(self, text: str) -> str:
        """
        Yalnız uzun bir mətn parçasını (transkript deyil) xülasə etmək üçün istifadə olunur.
        """
        system_prompt = (
            "Sən mətnləri qısa və konkret xülasə etmək üçün ixtisaslaşmış modelisən. "
            "Cavabını Azərbaycan dilində ver."
        )
        user_prompt = f"Aşağıdakı mətni qısa, nöqtəli bəndlərlə xülasə et:\n\n{text}"

        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt}
            ],
            "max_tokens": 1024,
            "temperature": 0.3,
            "stream": False
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        resp = requests.post(self.api_url, headers=headers, json=payload)
        if resp.status_code != 200:
            logger.error("DeepSeek API error %s: %s", resp.status_code, resp.text)
            raise RuntimeError("DeepSeek API error")

        return resp.json()["choices"][0]["message"]["content"]
