import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import re

def get_gsheet_data(spreadsheet_id, sheet_name=None, credentials_file='credentials.json'):
    """Получает данные из Google Sheets и возвращает DataFrame."""
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(spreadsheet_id)
    if sheet_name:
        worksheet = sheet.worksheet(sheet_name)
    else:
        worksheet = sheet.sheet1

    data = worksheet.get_all_values()
    if not data:
        return pd.DataFrame()

    headers = data[0]
    rows = data[1:]
    return pd.DataFrame(rows, columns=headers)


def write_df_to_sheet(df, spreadsheet_id, sheet_name, start_cell='A1', include_headers=False, credentials_file='credentials.json'):
    """Записывает DataFrame в Google Sheets, начиная с указанной ячейки."""
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # Подготовка данных
    if include_headers:
        data = [df.columns.tolist()] + df.values.tolist()
    else:
        data = df.values.tolist()

    rows = len(data)
    cols = len(data[0]) if data else 0
    if rows == 0 or cols == 0:
        print('Нет данных для записи')
        return

    # Парсим начальную ячейку
    match = re.match(r'([A-Z]+)(\d+)', start_cell.upper())
    if not match:
        raise ValueError(f'Некорректный адрес ячейки: {start_cell}')
    start_col_letter = match.group(1)
    start_row = int(match.group(2))

    start_col = gspread.utils.column_letter_to_index(start_col_letter)  # 1-индексация
    end_row = start_row + rows - 1
    end_col = start_col + cols - 1
    end_cell = gspread.utils.rowcol_to_a1(end_row, end_col)
    cell_range = f'{start_cell}:{end_cell}'

    # Очищаем старые данные
    sheet.batch_clear([cell_range])

    # Записываем новые данные
    sheet.update(cell_range, data, value_input_option='USER_ENTERED')
    print(f'Записано {rows} строк в {sheet_name}, начиная с {start_cell}')