import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")


def fetch_board_data(board_id):
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": MONDAY_API_KEY,
        "Content-Type": "application/json",
    }

    query = """
    query ($board_id: [ID!]!) {
      boards(ids: $board_id) {
        items_page(limit: 500) {
          items {
            name
            column_values {
              column { title }
              text
            }
          }
        }
      }
    }
    """

    variables = {"board_id": int(board_id)}

    response = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers=headers,
    )

    data = response.json()

    try:
        items = data["data"]["boards"][0]["items_page"]["items"]
    except:
        return pd.DataFrame()

    records = []

    for item in items:
        row = {"item name": item["name"]}
        for col in item["column_values"]:
            col_name = col["column"]["title"]
            row[col_name] = col["text"]
        records.append(row)

    return pd.DataFrame(records)