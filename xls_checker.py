import pandas as pd
from logging_config import log

def format_xls():
    
    try:
        df_list = pd.read_html(f'downloads\\EGXMC.xls')

        # If there's more than one table, you might need to specify the correct one
        df = df_list[0]
        # Rename the columns from Arabic to English
        df.rename(columns={'تاريخ التداول': 'Date', 'إجمالي رأس المال السوقي': 'Value'}, inplace=True)

        # Convert the 'Date' column to datetime, coercing errors to NaT (Not a Time)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)

        df['Date']=df['Date'].dt.date

        # Convert the 'Value' column to string with commas as thousand separators
        df['Value'] = df['Value'].apply(lambda x: '{:,.0f}'.format(x))

        # Save back to Excel (as .xls or .xlsx)
        df.to_excel(f'data\\MC.xlsx', index=False, engine='openpyxl')

        log.info('data saved [green]successfully![/green]')
    except Exception as e:
        log.warning(e)
#format_xls()        