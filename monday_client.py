"""
Monday.com API Client - Read-only access to boards
"""
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

class MondayClient:
    def __init__(self):
        self.api_key = os.getenv('eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYxOTg0MDg4MywiYWFpIjoxMSwidWlkIjo5OTcwNjQwMywiaWFkIjoiMjAyNi0wMi0xMVQwNjo0Njo1Ni4xNzRaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MzM3NDgzNTcsInJnbiI6ImFwc2UyIn0.R8Q599ZxZq4YI9hRvqwjw1Cg60TNj3MKNqRcK_TKKWI')
        self.api_url = "https://api.monday.com/v2"
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "API-Version": "2024-01"
        }
    
    def execute_query(self, query):
        """Execute GraphQL query"""
        try:
            response = requests.post(
                self.api_url,
                json={'query': query},
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error: {e}")
            return None
    
    def get_boards(self):
        """Get all available boards"""
        query = """
        {
            boards {
                id
                name
                description
            }
        }
        """
        result = self.execute_query(query)
        return result.get('data', {}).get('boards', []) if result else []
    
    def get_board_items(self, board_id):
        """Get all items from a specific board"""
        query = f"""
        {{
            boards(ids: {board_id}) {{
                items_page(limit: 500) {{
                    items {{
                        id
                        name
                        column_values {{
                            id
                            text
                            value
                        }}
                    }}
                }}
            }}
        }}
        """
        result = self.execute_query(query)
        
        if not result or 'errors' in result:
            return []
        
        boards = result.get('data', {}).get('boards', [])
        if not boards:
            return []
        
        items = boards[0].get('items_page', {}).get('items', [])
        
        # Convert to DataFrame-friendly format
        processed_items = []
        for item in items:
            row = {'id': item['id'], 'name': item['name']}
            for col in item.get('column_values', []):
                row[col['id']] = col['text']
                row[f"{col['id']}_raw"] = col['value']
            processed_items.append(row)
        
        return processed_items
    
    def get_board_columns(self, board_id):
        """Get column structure of a board"""
        query = f"""
        {{
            boards(ids: {board_id}) {{
                columns {{
                    id
                    title
                    type
                }}
            }}
        }}
        """
        result = self.execute_query(query)
        boards = result.get('data', {}).get('boards', [])
        return boards[0].get('columns', []) if boards else []
    
    def find_board_by_name(self, name_pattern):
        """Find board ID by name pattern"""
        boards = self.get_boards()
        for board in boards:
            if name_pattern.lower() in board['name'].lower():
                return board['id']
        return None