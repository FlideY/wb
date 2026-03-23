# %% [markdown]
# # Описание функций
# 
# - `get_gsheet_data(spreadsheet_id, sheet_name=None, credentials_file='credentials.json')`  
#   Загружает данные из Google Sheets и возвращает pandas DataFrame.  
#   Использует сервисный аккаунт из указанного JSON-файла.
# 
# - `write_df_to_sheet(df, spreadsheet_id, sheet_name, start_cell='A1', include_headers=False, credentials_file='credentials.json')`  
#   Записывает DataFrame в Google Sheets, начиная с заданной ячейки.  
#   Очищает целевой диапазон перед записью.

# %%
import pandas as pd
from datetime import datetime, timedelta
from gsheets_utils import get_gsheet_data, write_df_to_sheet
import os
# creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')

# %% [markdown]
# # Конфигурация и загрузка данных
# 
# Здесь задаются ID таблиц, имена листов и путь к файлу credentials.json.
# Затем загружаются три источника: операции, соответствие (qr - barcode, из табл. БазаДанныхФедорович), товары.

# %%

if __name__ == "__main__":
    # ID таблиц
    source_id = '1B7Yw6rn5HZrDmRUc-bhNiCx9_JQWdoYDVMoLamHAckc'   # операции + себестоимость
    target_id = '1mk0b6ZUSbgCOdmwXaqReu_7_BPlSACg90uZ2ZH9_zrA'  # таблица для записи
    db_id = '147Y4Vghqh1-49QyWcX_9vbHOGQJUUjVrp8WTFy_bGJ4'      # товары

    # Имена листов
    sheet_operations = 'Операции_daily'
    sheet_match = 'Себестоимость'
    sheet_products = 'product'
    sheet_target = 'ТеплQrКомиссия'

    creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')

    # Загрузка
    print("Загрузка данных...")
    operations = get_gsheet_data(source_id, sheet_operations, creds_file)
    match = get_gsheet_data(source_id, sheet_match, creds_file)
    products = get_gsheet_data(db_id, sheet_products, creds_file)

    print(f"Операции: {operations.shape}, Себестоимость: {match.shape}, Товары: {products.shape}")

    # %% [markdown]
    # # Расчёт
    # 
    # Объединение данных, фильтрация, расчёт агрегированных и дневных показателей.

    # %%
    # Соединяем операции с себестоимостью по штрихкоду
    df = pd.merge(operations, match, how='left', on='barcode')
    df = df[['qr', 'costPrice', 'supplier_oper_name', 'rr_dt',
            'retail_amount', 'ppvz_for_pay', 'ppvz_spp_prc']]

    # Преобразование типов (замена запятых на точки)
    for col in ['ppvz_for_pay', 'ppvz_spp_prc', 'retail_amount']:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    # Приводим даты к типу datetime (без времени)
    df['rr_dt'] = pd.to_datetime(df['rr_dt']).dt.normalize()

    # Последние 30 дней от максимальной даты, исключая самый старый день
    max_date = df['rr_dt'].max()
    threshold = max_date - timedelta(days=30)

    df_filtered = df[(df['rr_dt'] > threshold) & (df['rr_dt'] <= max_date)].copy()
    valid_ops = ['Продажа', 'Возврат', 'Логистика']
    df_filtered = df_filtered[df_filtered['supplier_oper_name'].isin(valid_ops)]

    print(f"После фильтрации: {df_filtered.shape}")

    # Вспомогательные колонки
    df_filtered['rk_value'] = 0.0
    mask = df_filtered['retail_amount'] > 0
    df_filtered.loc[mask, 'rk_value'] = (1 - df_filtered.loc[mask, 'ppvz_for_pay'] / 
                                        df_filtered.loc[mask, 'retail_amount']) * 100

    df_filtered['contribution'] = 0.0
    df_filtered.loc[df_filtered['supplier_oper_name'] == 'Продажа', 'contribution'] = (
        df_filtered['retail_amount'] - df_filtered['ppvz_for_pay']
    )
    df_filtered.loc[df_filtered['supplier_oper_name'] == 'Возврат', 'contribution'] = (
        -(df_filtered['retail_amount'] - df_filtered['ppvz_for_pay'])
    )
    df_filtered.loc[df_filtered['supplier_oper_name'] == 'Логистика', 'contribution'] = (
        -df_filtered['ppvz_for_pay']
    )

    # Группировка
    grouped = df_filtered.groupby('qr').agg(
        СредРеалюмиссия=('contribution', 'sum'),
        СредняяРеальнаяКомиссия=(
            'rk_value',
            lambda x: x[df_filtered.loc[x.index, 'supplier_oper_name'] != 'Логистика'].mean()
        ),
        Средспп=(
            'ppvz_spp_prc',
            lambda x: x[df_filtered.loc[x.index, 'supplier_oper_name'] != 'Логистика'].mean()
        )
    )
    grouped['Средквб'] = grouped['СредняяРеальнаяКомиссия'] + grouped['Средспп']
    grouped = grouped.fillna(0)

    valid_qr = grouped.index
    print(f"Сформировано {len(valid_qr)} строк группированных показателей")

    # Оставляем только те qr, которые есть в агрегированных данных
    df_daily = df_filtered[df_filtered['qr'].isin(valid_qr)].copy()

    # Агрегация по qr и дате (только продажи/возвраты)
    daily_agg = (
        df_daily[df_daily['supplier_oper_name'] != 'Логистика']
        .groupby(['qr', 'rr_dt'])
        .agg(
            RK=('rk_value', 'mean'),
            SPP=('ppvz_spp_prc', 'mean')
        )
        .reset_index()
    )
    daily_agg['TOTAL'] = daily_agg['RK'] + daily_agg['SPP']

    # Все комбинации qr x дата
    all_qr = df_daily['qr'].unique()
    date_range = pd.date_range(start=threshold + timedelta(days=1), end=max_date, freq='D')
    date_range = pd.DatetimeIndex(date_range).normalize()

    full_index = pd.MultiIndex.from_product([all_qr, date_range], names=['qr', 'rr_dt'])
    full_df = pd.DataFrame(index=full_index).reset_index()
    full_df['rr_dt'] = pd.to_datetime(full_df['rr_dt']).dt.normalize()
    daily_agg['rr_dt'] = pd.to_datetime(daily_agg['rr_dt']).dt.normalize()

    daily_full = full_df.merge(daily_agg, on=['qr', 'rr_dt'], how='left')
    daily_full[['RK', 'SPP', 'TOTAL']] = daily_full[['RK', 'SPP', 'TOTAL']].fillna(0)

    # Широкий формат (три колонки на дату)
    pivot_rk = daily_full.pivot(index='qr', columns='rr_dt', values='RK')
    pivot_spp = daily_full.pivot(index='qr', columns='rr_dt', values='SPP')
    pivot_total = daily_full.pivot(index='qr', columns='rr_dt', values='TOTAL')

    pivot_rk = pivot_rk.sort_index(axis=1)
    pivot_spp = pivot_spp.sort_index(axis=1)
    pivot_total = pivot_total.sort_index(axis=1)

    wide_df = pd.DataFrame(index=pivot_rk.index)
    for date in pivot_rk.columns:
        wide_df[(date, 'RK')] = pivot_rk[date]
        wide_df[(date, 'SPP')] = pivot_spp[date]
        wide_df[(date, 'TOTAL')] = pivot_total[date]

    # Преобразуем мультииндекс в плоские имена
    wide_df.columns = [f'{col[0].strftime("%Y-%m-%d")}_{col[1]}' for col in wide_df.columns]

    # Сохраняем порядок qr для синхронизации с другими частями
    qr_list = wide_df.index.tolist()
    wide_df = wide_df.reset_index(drop=True)

    print(f"Сформирован wide DataFrame: {wide_df.shape[0]} строк, {wide_df.shape[1]} столбцов")


    # Фильтруем товары по qr, которые есть в расчётах, и приводим к порядку qr_list
    products_filtered = products[products['qr'].isin(qr_list)].copy()
    products_filtered = products_filtered.set_index('qr').loc[qr_list].reset_index()

    # Переименовываем колонки в соответствии с заголовками на листе
    col_mapping = {
        'qr': '#QR',
        'create_date': '#ДатаЗаведенияТовара',
        'se': '#Сборный/Элементарный',
        'product_type': '#ТипТовара',
        'product': '#Товар',
        'link_photo': '#СсылкаНаФотоТовара'
    }
    existing_mapping = {k: v for k, v in col_mapping.items() if k in products_filtered.columns}
    products_part = products_filtered[list(existing_mapping.keys())].rename(columns=existing_mapping)

    # Добавляем колонку с формулой для отображения изображения (ссылается на ячейку слева)
    products_part['#Фототовара'] = '=IMAGE(INDIRECT("RC[-1]"; FALSE))'

    print(f"Товарная часть: {products_part.shape}")

    # Приводим агрегированные показатели к тому же порядку строк
    grouped_aligned = grouped.loc[qr_list].reset_index(drop=True)

    # Объединяем всё по горизонтали
    final_df = pd.concat([products_part, grouped_aligned, wide_df], axis=1)

    print(f"Итоговый DataFrame: {final_df.shape[0]} строк, {final_df.shape[1]} столбцов")

    # %% [markdown]
    # # Запись результатов
    # 
    # Итоговый DataFrame записывается в Google Sheets одним вызовом.
    # Заголовки уже подготовлены на листе, поэтому `include_headers=False`.

    # %%
    write_df_to_sheet(
        df=final_df,
        spreadsheet_id=target_id,
        sheet_name=sheet_target,
        start_cell='D4',
        include_headers=False,
        credentials_file=creds_file
    )

    print("Все данные записаны.")


