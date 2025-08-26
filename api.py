from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from search_products import search_products  # importa sua função já pronta

app = FastAPI()

class Query(BaseModel):
    query: str

@app.post("/search")
def search(q: Query):
    result = search_products(q.query, k=8)
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
