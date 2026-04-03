# %%
import pandas as pd
from datetime import datetime, timedelta
from gsheets_utils import get_gsheet_data, write_df_to_sheet
import json

# %%
# ID таблиц
source_id = '1B7Yw6rn5HZrDmRUc-bhNiCx9_JQWdoYDVMoLamHAckc'   # операции + себестоимость
target_id = '1mk0b6ZUSbgCOdmwXaqReu_7_BPlSACg90uZ2ZH9_zrA'  # таблица для записи
db_id = '147Y4Vghqh1-49QyWcX_9vbHOGQJUUjVrp8WTFy_bGJ4'      # товары

# Имена листов
sheet_operations = 'Операции_daily'
sheet_match = 'Себестоимость'
sheet_products = 'product'
sheet_target = 'ТеплBcКомиссия'
sheet_cards = 'Карточки WB'

creds_file = 'credentials.json'

# Загрузка
print("Загрузка данных...")
operations = get_gsheet_data(source_id, sheet_operations, creds_file)
match = get_gsheet_data(source_id, sheet_match, creds_file)
products = get_gsheet_data(db_id, sheet_products, creds_file)
cards = get_gsheet_data(source_id, sheet_cards, creds_file)

print(f"Операции: {operations.shape}, Себестоимость: {match.shape}, Товары: {products.shape}, Карточки: {cards.shape}")
# print(type(cards['photos'].iloc[0]))

# %%
df = pd.merge(operations, match, how='left', on='barcode')
df = pd.merge(df, products, how='left', on='qr')
df = pd.merge(df, cards, how='left', left_on='nm_id', right_on='nmID')
# df = df[['qr', 'barcode', 'costPrice', 'supplier_oper_name', 'rr_dt',
#          'retail_amount', 'ppvz_for_pay', 'ppvz_spp_prc', 'nmId', 'subject_name',
#          ]]
# print(df.first)

# После выполнения всех merge и получения df

# -------------------------------------------------------------------
# 1. Предварительная обработка: нужные колонки, типы, фильтрация
# -------------------------------------------------------------------
# Убедимся, что есть все необходимые поля
required_cols = ['barcode', 'qr', 'nmID', 'sa_name', 'subject_name', 'create_date', 'se', 'product_type', 
                 'product', 'link_photo', 'createdAt', 'updatedAt', 'description', 'photos',
                 'supplier_oper_name', 'rr_dt', 'retail_amount', 'ppvz_for_pay', 'ppvz_spp_prc']
# Проверим наличие, но продолжим с тем, что есть

# Преобразование типов
for col in ['ppvz_for_pay', 'ppvz_spp_prc', 'retail_amount']:
    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

df['rr_dt'] = pd.to_datetime(df['rr_dt']).dt.normalize()

# Фильтр по последним 30 дням (от максимальной даты)
max_date = df['rr_dt'].max()
threshold = max_date - timedelta(days=30)
df = df[(df['rr_dt'] > threshold) & (df['rr_dt'] <= max_date)].copy()
df = df[df['supplier_oper_name'].isin(['Продажа', 'Возврат', 'Логистика'])]

# Вспомогательные колонки
df['rk_value'] = 0.0
mask = df['retail_amount'] > 0
df.loc[mask, 'rk_value'] = (1 - df.loc[mask, 'ppvz_for_pay'] / df.loc[mask, 'retail_amount']) * 100

df['contribution'] = 0.0
df.loc[df['supplier_oper_name'] == 'Продажа', 'contribution'] = (
    df['retail_amount'] - df['ppvz_for_pay']
)
df.loc[df['supplier_oper_name'] == 'Возврат', 'contribution'] = (
    -(df['retail_amount'] - df['ppvz_for_pay'])
)
df.loc[df['supplier_oper_name'] == 'Логистика', 'contribution'] = (
    -df['ppvz_for_pay']
)

# -------------------------------------------------------------------
# 2. Агрегация по barcode (средние показатели)
# -------------------------------------------------------------------
grouped = df.groupby('barcode').agg(
    СредРеалКомиссияРуб=('contribution', 'sum'),          # #(Р)CредРеалКомиссия(РК)
    СредняяРеальнаяКомиссия=(
        'rk_value',
        lambda x: x[df.loc[x.index, 'supplier_oper_name'] != 'Логистика'].mean()
    ),
    СредСПП=(
        'ppvz_spp_prc',
        lambda x: x[df.loc[x.index, 'supplier_oper_name'] != 'Логистика'].mean()
    )
)
grouped['СредКВВ'] = grouped['СредняяРеальнаяКомиссия'] + grouped['СредСПП']
grouped = grouped.fillna(0)

valid_barcode = grouped.index.tolist()
df_daily = df[df['barcode'].isin(valid_barcode)].copy()

# -------------------------------------------------------------------
# 3. Дневные значения RK, SPP, TOTAL (только продажи/возвраты)
# -------------------------------------------------------------------
daily_agg = (
    df_daily[df_daily['supplier_oper_name'] != 'Логистика']
    .groupby(['barcode', 'rr_dt'])
    .agg(RK=('rk_value', 'mean'), SPP=('ppvz_spp_prc', 'mean'))
    .reset_index()
)
daily_agg['TOTAL'] = daily_agg['RK'] + daily_agg['SPP']

# Все комбинации barcode x дата
all_barcode = df_daily['barcode'].unique()
date_range = pd.date_range(start=threshold + timedelta(days=1), end=max_date, freq='D')
date_range = pd.DatetimeIndex(date_range).normalize()

full_index = pd.MultiIndex.from_product([all_barcode, date_range], names=['barcode', 'rr_dt'])
full_df = pd.DataFrame(index=full_index).reset_index()
full_df['rr_dt'] = pd.to_datetime(full_df['rr_dt']).dt.normalize()
daily_agg['rr_dt'] = pd.to_datetime(daily_agg['rr_dt']).dt.normalize()

daily_full = full_df.merge(daily_agg, on=['barcode', 'rr_dt'], how='left')
daily_full[['RK', 'SPP', 'TOTAL']] = daily_full[['RK', 'SPP', 'TOTAL']].fillna(0)

# Широкий формат
pivot_rk = daily_full.pivot(index='barcode', columns='rr_dt', values='RK')
pivot_spp = daily_full.pivot(index='barcode', columns='rr_dt', values='SPP')
pivot_total = daily_full.pivot(index='barcode', columns='rr_dt', values='TOTAL')

pivot_rk = pivot_rk.sort_index(axis=1)
pivot_spp = pivot_spp.sort_index(axis=1)
pivot_total = pivot_total.sort_index(axis=1)

wide_df = pd.DataFrame(index=pivot_rk.index)
for date in pivot_rk.columns:
    wide_df[(date, 'RK')] = pivot_rk[date]
    wide_df[(date, 'SPP')] = pivot_spp[date]
    wide_df[(date, 'TOTAL')] = pivot_total[date]

wide_df.columns = [f'{col[0].strftime("%Y-%m-%d")}_{col[1]}' for col in wide_df.columns]
barcode_list = wide_df.index.tolist()
wide_df = wide_df.reset_index(drop=True)

# -------------------------------------------------------------------
# 4. Товарные и дополнительные данные (приводим к порядку barcode_list)
# -------------------------------------------------------------------
# Уникальное соответствие barcode -> qr (берём первое вхождение)
barcode_to_qr = df[['barcode', 'qr']].drop_duplicates(subset='barcode', keep='first')
qr_list = barcode_to_qr.set_index('barcode').loc[barcode_list, 'qr'].values

# Товарные данные из products (уже есть в df, но возьмём уникальные по qr)
products_unique = df[['qr', 'create_date', 'se', 'product_type', 'product', 'link_photo']].drop_duplicates(subset='qr', keep='first')
products_unique = products_unique.set_index('qr').loc[qr_list].reset_index()

# Переименовываем согласно желаемым заголовкам
products_part = pd.DataFrame()
products_part['#QR'] = products_unique['qr']
products_part['#ДатаЗаведенияТовара'] = products_unique['create_date']
products_part['#Сборный/Элементарный'] = products_unique['se']
products_part['#ТипТовара'] = products_unique['product_type']
products_part['#Товар'] = products_unique['product']
products_part['#СсылкаНаФотоТовара'] = products_unique['link_photo']
products_part['#ФотоТовара'] = '=IMAGE(INDIRECT("RC[-1]"; FALSE))'


# Данные из cards (по nmID) – для каждого barcode возьмём соответствующий nmID
barcode_to_nmid = df[['barcode', 'nmID']].drop_duplicates(subset='barcode', keep='first')
cards_unique = df[['nmID', 'subject_name', 'createdAt', 'updatedAt', 'description', 'photos']].drop_duplicates(subset='nmID', keep='first')
# Привязываем к barcode
cards_by_barcode = barcode_to_nmid.merge(cards_unique, on='nmID', how='left').set_index('barcode').loc[barcode_list]
# Создаём отображение barcode -> sa_name (берём первое вхождение)
barcode_to_sa = df[['barcode', 'sa_name']].drop_duplicates(subset='barcode', keep='first')

# Формируем дополнительные колонки
extra = pd.DataFrame()
extra['#Кабинет'] = ''
extra['#ДатаСоздания'] = cards_by_barcode['createdAt'].values
extra['#ДатаИзменения'] = cards_by_barcode['updatedAt'].values
extra['#Баркод'] = barcode_list
extra['#АртикулSCU'] = cards_by_barcode['nmID'].values
extra['#Категория'] = cards_by_barcode['subject_name'].values
# ИСПРАВЛЕНО: берём sa_name вместо qr
extra['#АртикулПродавца'] = barcode_to_sa.set_index('barcode').loc[barcode_list, 'sa_name'].values

# --- Извлечение ссылки на фото из JSON-строки ---
# Убедимся, что json импортирован (в начале файла должен быть import json)

def extract_photo_safe(photos_val):
    # Обработка пустых значений
    if pd.isna(photos_val) or photos_val == '':
        return ''
    try:
        # Если значение уже список (например, после предыдущего парсинга)
        if isinstance(photos_val, list) and len(photos_val) > 0:
            return photos_val[0].get('big', '')
        # Если строка, парсим JSON
        if isinstance(photos_val, str):
            data = json.loads(photos_val)
            if isinstance(data, list) and len(data) > 0:
                # Поле может называться 'big' или 'url' — проверяем оба
                if 'big' in data[0]:
                    return data[0]['big']
                elif 'url' in data[0]:
                    return data[0]['url']
    except Exception as e:
        # Для отладки можно раскомментировать:
        # print(f"Ошибка парсинга: {e}, значение: {str(photos_val)[:100]}")
        pass
    return ''

# Применяем функцию к колонке photos
cards_by_barcode['photo_link'] = cards_by_barcode['photos'].apply(extract_photo_safe)

# Заполняем колонки в extra
extra['#СсылкаНаФото'] = cards_by_barcode['photo_link'].values
# Формула IMAGE: используем запятую в качестве разделителя аргументов (для некоторых локалей)
extra['#Фото'] = '=IMAGE(INDIRECT("RC[-1]"; FALSE))'
# -------------------------------------------------------------------
# 5. Агрегированные показатели (по barcode_list)
# -------------------------------------------------------------------
grouped_aligned = grouped.loc[barcode_list].reset_index(drop=True)
# Переименовываем колонки под требуемые названия
grouped_aligned.columns = ['#(Р)CредРеалКомиссия(РК)', '#(%)СредняяРеальнаяКомиссия', '#(%)СредСПП', '#(%)СредКВВ']

# -------------------------------------------------------------------
# 6. Финальное объединение
# -------------------------------------------------------------------
final_df = pd.concat([products_part, extra, grouped_aligned, wide_df], axis=1)
final_df = final_df.fillna('')

print(f"Итоговый DataFrame: {final_df.shape[0]} строк, {final_df.shape[1]} столбцов")
print("Порядок колонок:", final_df.columns.tolist())

# -------------------------------------------------------------------
# 7. Запись в Google Sheets (при необходимости)
# -------------------------------------------------------------------
write_df_to_sheet(
    df=final_df,
    spreadsheet_id=target_id,
    sheet_name=sheet_target,
    start_cell='D5',
    include_headers=False,
    credentials_file=creds_file
)
print("Данные записаны.")


