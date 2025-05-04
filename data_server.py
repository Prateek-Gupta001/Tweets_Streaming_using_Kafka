import pandas as pd 
import time
import uvicorn
import fastapi
from fastapi import FastAPI
from fastapi.responses import JSONResponse




app = FastAPI()


df = pd.read_parquet("tweets.parquet",engine="pyarrow")

col_names = [
    "class",
    "user_id",
    "created_at",
    "search_query",
    "username",
    "tweet_text"
]
df.columns = col_names

filtered = df[df["class"] == 4]
filtered_2 = df[df["class"]== 0]

print(filtered.shape)
print(filtered_2.shape)
shuffled_df = df.sample(frac=1).reset_index(drop=True)
shuffled_df["class"] = shuffled_df["class"].replace({0: -1, 4: 1})
print(shuffled_df.head(10))



@app.get("/data")
async def get_data(batch_size=100000):
    sample_df = shuffled_df.sample(n=100000)
    records = sample_df.to_dict(orient="records")
    print(f"Len of records {len(records)}")
    # return it directly (FastAPI will JSON‑encode a list of dicts just fine)
    print(f"First record is : {records[0]}")
    return JSONResponse(content=records)



def main():
    uvicorn.run("data_server:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()



