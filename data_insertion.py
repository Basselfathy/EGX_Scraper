import sqlite3
import os
import pandas as pd
from datetime import datetime
from logging_config import log

# Define file paths
script_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(script_dir, 'data', 'MC.db')
EXCEL_PATH = os.path.join(script_dir, 'data', 'MC.xlsx')


def remove_last_rows(num: int) -> None:
    """Remove the last 'num' rows from the Excel file."""
    try:
        df = pd.read_excel(EXCEL_PATH)
        df.drop(df.tail(num).index, inplace=True)
        df.to_excel(EXCEL_PATH, index=False)
        log.info(f"{num} line(s) dropped and Excel file updated!")
    except Exception as e:
        log.error(f"[red]Error removing rows from Excel[/red]: {e}")


def format_date_and_value(date: str, value: str) -> tuple[str, int]:
    """Format date and value for database insertion."""
    try:
        formatted_date = datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d")
        formatted_value = int(value.replace(",", ""))
        return formatted_date, formatted_value
    except Exception as e:
        log.error(f"[red]Error formatting date or value[/red]: {e}")
        raise


def insert_data_into_db(date: str, value: str) -> None:
    """Insert a new record into the database, avoiding duplicates."""
    formatted_date, formatted_value = format_date_and_value(date, value)
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Insert data using INSERT OR IGNORE to avoid duplicates
        cursor.execute(
            "INSERT OR IGNORE INTO market_capital_data (Date, Value) VALUES (?, ?)",
            (formatted_date, formatted_value)
        )
        conn.commit()

        if cursor.rowcount == 0:
            log.info(f"[yellow]Data for {formatted_date} already exists in [underline blue]{DB_PATH}[/underline blue][/yellow]")
        else:
            log.info(f"Data for {formatted_date} inserted [green]successfully[/green] into [underline blue]{DB_PATH}[/underline blue]")
    except sqlite3.IntegrityError as e:
        log.error(f"Database insertion error: {e}")
    finally:
        conn.close()


def insert_data_into_excel(date: str, value: str) -> None:
    """Insert a new record into the Excel file, avoiding duplicates."""
    formatted_date, _ = format_date_and_value(date, value)  # Only format date, use raw value
    
    if not os.path.exists(EXCEL_PATH):
        log.error(f"[red]Excel file not found at: [underline blue]{EXCEL_PATH}[/underline blue][/red]")
        return

    try:
        # Load the Excel file into a pandas DataFrame
        df = pd.read_excel(EXCEL_PATH)

        # Ensure 'Date' column is in string format (to match with new entries)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        # Check for duplicate entry
        if formatted_date in df['Date'].values:
            log.info(f"[yellow]Data for {formatted_date} already exists in [underline blue]{EXCEL_PATH}[/underline blue][/yellow]")
            return

        # Append new data to the DataFrame
        new_row = pd.DataFrame({'Date': [formatted_date], 'Value': [value]})
        df = pd.concat([df, new_row], ignore_index=True)

        # Save the updated DataFrame back to the Excel file
        df.to_excel(EXCEL_PATH, index=False)
        log.info(f"Data for {formatted_date} inserted [green]successfully[/green] into [underline blue]{EXCEL_PATH}[/underline blue]")

    except Exception as e:
        log.error(f"[red]Error updating Excel file[/red]: {e}")
