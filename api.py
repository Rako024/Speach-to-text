from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import logging
import psycopg2

# FastAPI instance
app = FastAPI()

# DeepSeek API endpoint və API Key
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"  # DeepSeek chat API endpoint
DEEPSEEK_API_KEY = "sk-415ea7ff259945b386d57c216e2bc77d"  # DeepSeek API açarınızı buraya əlavə edin

# Loglama üçün konfiqurasiya
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic model for incoming analyze request
class AnalyzeRequest(BaseModel):
    start_time: str
    end_time: str

# Verilənlər bazasına qoşulma funksiyası
def get_db_connection():
    conn = psycopg2.connect(
        dbname="speach_to_text", user="postgres", password="!2627251Rr", host="localhost"
    )
    return conn

# Xülasə yaratmaq üçün DeepSeek API-ə sorğu göndərmək üçün funksiya
def get_summary_from_deepseek(text: str):
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    data = {
        "messages": [
            {"content": "You are a helpful assistant", "role": "system"},
            {"content": text, "role": "user"}
        ],
        "model": "deepseek-chat",
        "frequency_penalty": 0,
        "max_tokens": 2048,
        "presence_penalty": 0,
        "response_format": {"type": "text"},
        "stop": None,
        "stream": False,
        "temperature": 1,
        "top_p": 1,
        "tools": None,
        "tool_choice": "none",
        "logprobs": False,
        "top_logprobs": None
    }
    
    try:
        logger.info("Sending request to DeepSeek API with text.")
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=data)
        logger.info(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Received summary: {result['choices'][0]['message']['content']}")
            return result['choices'][0]['message']['content']
        else:
            logger.error(f"Error response: {response.text}")
            raise HTTPException(status_code=response.status_code, detail=f"Error from DeepSeek API: {response.text}")
    except Exception as e:
        logger.error(f"Exception during API request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during summarization: {str(e)}")

# Zaman aralığına uyğun mətni əldə etmək üçün funksiya
def get_text_from_db(start_time: str, end_time: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT text FROM transcripts WHERE start_time >= %s AND end_time <= %s"
    cursor.execute(query, (start_time, end_time))
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="No transcripts found in this range")
    
    # Mətnləri birləşdiririk
    full_text = " ".join([row[0] for row in rows])
    return full_text

@app.post("/analyze/")
async def analyze_text(request: AnalyzeRequest):
    # Verilənlər bazasından mətni əldə edirik
    try:
        full_text = get_text_from_db(request.start_time, request.end_time)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching text: {str(e)}")
    
    # DeepSeek API ilə xülasə alınır
    try:
        final_summary = get_summary_from_deepseek(full_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during summarization: {str(e)}")
    
    return {"summary": final_summary}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
