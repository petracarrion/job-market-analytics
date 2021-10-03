import pandas as pd


def load_urls_as_db():
    return pd.read_csv("data/results/urls.csv")


df = load_urls_as_db()
url_split = df["url"].str.split("--", expand=True)
df["job_name_slug"] = url_split[1]
df["job_id"] = url_split[2]
df = df.sort_values(by=["job_id"], ascending=False)
df = df.reset_index(drop=True)
print(df)