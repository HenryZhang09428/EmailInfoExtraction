import json
from dotenv import load_dotenv
from core.llm import LLMClient

load_dotenv()

llm = LLMClient()
result = llm.chat_json('只输出JSON：{"ok": true}')
print(json.dumps(result, ensure_ascii=False, indent=2))
