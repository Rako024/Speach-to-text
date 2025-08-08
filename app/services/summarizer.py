# app/services/summarizer.py

import requests
import logging
from typing import List, Optional
from app.config import Settings
from app.api.schemas import SegmentInfo

logger = logging.getLogger(__name__)

class DeepSeekClient:
    """
    DeepSeek API ilə əlaqə saxlayır, həm tam transkriptləri,
    həm də açar sözə fokuslanmış xülasələri hazırlayır.
    """

    def __init__(self, settings: Settings):
        self.api_url = settings.deepseek_api_url
        self.api_key = settings.deepseek_key
        # Konfiqurable timeout (saniyə) – istəsən Settings-ə də çıxara bilərik
        self.timeout = 30

    def summarize(
        self,
        segments: List[SegmentInfo],
        keyword: Optional[str] = None
    ) -> str:
        """
        Açar sözlə axtarış nəticəsində əldə olunmuş SegmentInfo-ları
        birləşdirib xülasə verir. 
        - Əgər `keyword` verilibsə, hər hit üçün [fayl_adı +offset] və sentiment
          (pozitiv/negativ/neytral) göstərilir, sonra yekun xülasə.
        - Keyword verilməyibsə, ümumi nöqtəli bəndli xülasə qaytarır.
        """
        # Seqmentləri formatlı mətn halına gətiririk
        formatted = "\n\n".join(
            f"[{i+1}] ({seg.segment_filename} +{seg.offset_secs:.1f}s): {seg.text}"
            for i, seg in enumerate(segments)
        )

        # Əsas sistem promptu
        base_system = (
            "Sən transkript seqmentlərini başa düşən və onları strukturlaşdırılmış, "
            "aydın xülasə edən modelsən. Cavabını Azərbaycan dilində ver."
        )

        if keyword:
            system_prompt = (
                base_system +
                " Axtarılan söz “{kw}” üçün hər hit-də sentimenti (pozitiv/negativ/neytral) "
                "qiymətləndir və göstər."
            ).format(kw=keyword)
            user_prompt = (
                f"Aşağıda transkript seqmentləri var. Axtarılan söz “{keyword}”:\n\n"
                f"{formatted}\n\n"
                "1) Hər hit üçün `[fayl_adı +offset] cümlə` formatında yaz.\n"
                "2) Hər bir hit üçün sentiment (pozitiv/negativ/neytral) əlavə et.\n"
                "3) Sonda yekun xülasəni qısa, nöqtəli bəndlərlə ver."
            )
        else:
            system_prompt = base_system + " Aşağıdakı seqmentlərdən ümumi əsas məqamları çıxar."
            user_prompt = (
                "Aşağıdakı transkript seqmentlərini oxu və əsas məqamları "
                "qısa, nöqtəli bəndlərlə ver:\n\n" + formatted
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

        try:
            resp = requests.post(self.api_url, headers=headers, json=payload, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            logger.error("DeepSeek API request error: %s", e)
            # Fallback: modelləşdirilmiş sadə xülasə mesajı
            return "Xülasə xidmətinə qoşulmaq mümkün olmadı. Daha sonra yenidən cəhd edin."

        if resp.status_code != 200:
            # DeepSeek qeyri-200 cavabları (rate limit, auth və s.)
            logger.error("DeepSeek API error %s: %s", resp.status_code, resp.text)
            return "Xülasə alınarkən xidmət xəta verdi. Zəhmət olmasa sonra yenidən yoxlayın."

        try:
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error("DeepSeek API parse error: %s; body=%s", e, resp.text[:500])
            return "Xülasə nəticəsini emal etmək mümkün olmadı."
        

    def summarize_text(self, text: str) -> str:
        """
        Uzun bir mətn parçasını (transkript deyil) qısa xülasə etmək üçün.
        """
        system_prompt = (
            "Sən mətnləri qısa və konkret xülasə etmək üçün ixtisaslaşmış modelisən. Cavabını Azərbaycan dilində ver."
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

        try:
            resp = requests.post(self.api_url, headers=headers, json=payload, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            logger.error("DeepSeek API request error: %s", e)
            return "Xülasə xidmətinə qoşulmaq mümkün olmadı. Daha sonra yenidən cəhd edin."

        if resp.status_code != 200:
            logger.error("DeepSeek API error %s: %s", resp.status_code, resp.text)
            return "Xülasə alınarkən xidmət xəta verdi. Zəhmət olmasa sonra yenidən yoxlayın."

        try:
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error("DeepSeek API parse error: %s; body=%s", e, resp.text[:500])
            return "Xülasə nəticəsini emal etmək mümkün olmadı."
