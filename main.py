import pandas as pd
import pytz
import os
import time as time_module
from datetime import datetime, timedelta,time
import asyncio
from xls_checker import format_xls
from logging_config import log
# Import your scraper class
from EGXMC_Scraper import MarketCapitalScraper, MarketCapitalProcessor # Make sure to replace with the correct import


class MarketCapitalUpdater:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, 'data\\MC.db')
        self.excel_path = os.path.join(self.script_dir, 'data\\MC.xlsx')
        self.browser_path = os.path.join(self.script_dir, 'Chromium\\Application\\chrome.exe')
        self.url = 'https://www.egx.com.eg/ar/MCData.aspx'
        self.scraper = MarketCapitalScraper(self.url, self.browser_path)
        self.today_date = datetime.now().date()
        self.rd = None

    def check_time(self, hour:int , minute:int , second:int):
        EG_timezone = pytz.timezone('Egypt')
        time_now = datetime.now(EG_timezone)
        target_time = time(hour=hour, minute=minute, second=second)
        log.info(f"Time now is [yellow]{time_now.strftime('%H:%M:%S')}[/yellow] on Egyptian Timezone!")
        time_to_run = time_now.time() > target_time
        return time_to_run
    
    def check_excel_file_exists(self):
        return os.path.exists(self.excel_path)

    def load_excel(self):
        try:
            self.rd = pd.read_excel(self.excel_path)
            self.rd['Date'] = pd.to_datetime(self.rd['Date'])
            log.info("Excel file loaded [green]successfully[/green].")
        except Exception as e:
            log.error(f"Error loading Excel file: {e}")
            raise

    def get_last_record_date(self):
        return self.rd['Date'].iloc[-1].date() # type: ignore

    def calculate_expected_next_date(self, last_date):
        expected_next_date = last_date + timedelta(days=1)
        if last_date.weekday() == 3:  # Thursday
            expected_next_date += timedelta(days=2)  # Skip to Sunday
        elif last_date.weekday() == 4:  # Friday
            expected_next_date += timedelta(days=2)  # Skip to Sunday
        return expected_next_date

    def calculate_date_difference(self, last_date):
        expected_next_date = self.calculate_expected_next_date(last_date)
        difference = (self.today_date - expected_next_date).days
        return difference

    async def download_and_fix_excel(self):
        try:
            await self.scraper.setup_browser()
            await self.scraper.download_file()
            format_xls()
        except Exception as e:
            log.error(f"An error occurred during the download: {e}")
        finally:
            await self.scraper.close_browser()

    async def update_market_data(self):
        if self.check_time(14,31,00):
            log.info(f"[green]Initiate scraping process![/green]")
            last_date = self.get_last_record_date()
            difference = self.calculate_date_difference(last_date)
            
            if difference > 0:
                log.info(f"The last record's date is [yellow]more than one day[/yellow] behind today's date (excluding weekends).")
                await self.download_and_fix_excel()

            elif difference == 0:
                log.info("The last record's date is [yellow]one day[/yellow] behind today's date (excluding weekends).")
                await self.process_latest_data()
                
            elif self.today_date.strftime('%A') in ["Friday","Saturday"]:
                log.info(f"Today is \'{self.today_date.strftime('%A')}\' and it's a weekend, there will be no new Data!")

            else:
                log.info("The last record's date is [green]up-to-date[/green].")
        else:
            log.info(f"[red]Market hasn't been updated yet![/red]")

    async def process_latest_data(self):
        try:
            await self.scraper.setup_browser()
            await self.scraper.navigate_to_last_page()
            data = await self.scraper.extract_data()

            processor = MarketCapitalProcessor(data)
            latest_date, latest_value = processor.process_latest_data()

            if latest_date and latest_value:
                processor.save_to_database(latest_date, latest_value)
                processor.save_to_excel(latest_date, latest_value)
            else:
                log.error('No new data found.')
        except Exception as e:
            log.error(f"An error occurred during data processing: {e}")
        finally:
            await self.scraper.close_browser()

    async def run(self):
        start_time = time_module.time()

        if not self.check_excel_file_exists():
            log.error(f"No file found at {self.excel_path}, initiating the scraper to download the file.")
            await self.download_and_fix_excel()
        else:
            self.load_excel()
            await self.update_market_data()

        end_time = time_module.time()
        log.info(f'Execution time: {end_time - start_time:.2f} seconds')


# Run the main function
if __name__ == "__main__":
    updater = MarketCapitalUpdater()
    asyncio.run(updater.run())