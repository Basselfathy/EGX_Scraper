import asyncio
from pyppeteer import launch
from pyppeteer_stealth import stealth
import os
import shutil
import time
from lxml import etree
from data_insertion import insert_data_into_db, insert_data_into_excel 
from logging_config import log
#########################
#########################

class MarketCapitalScraper:
    def __init__(self, url, browser_path):
        self.url = url
        self.browser_path = browser_path
        self.browser = None
        self.page = None

    async def setup_browser(self):
        self.browser = await launch(
            headless=True,
            executablePath=self.browser_path,
            args=['--disable-dev-shm-usage', '--no-sandbox'],
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False
        )
        self.page = await self.browser.newPage()
        await stealth(self.page)
        log.info('Browser [green]launched![/green].')


    async def navigate_to_last_page(self):
        await self.page.goto(self.url)
        log.info('Page loaded successfully.')
        await asyncio.sleep(1)
        await self.page.waitForSelector('#ctl00_C_I_lnkLastPage', timeout=30000)
        log.info('Last page link found.')
        
        await self.page.evaluate('''() => {
            const lastPageLink = document.getElementById('ctl00_C_I_lnkLastPage');
            if (lastPageLink) {
                lastPageLink.click();
            } else {
                console.log('Pagination link not found');
            }
        }''')
        
        try:
            await self.page.waitForNavigation(timeout=30000)
            log.info('Navigation to the last page [green]successful[/green].')
        except asyncio.TimeoutError:
            log.warning('Navigation timeout. The page may not have loaded completely.')


    async def extract_data(self):
        if not self.page.isClosed():
            html_content = await self.page.content()

        parser = etree.HTMLParser()
        tree = etree.fromstring(html_content, parser)

        dates = tree.xpath("//span[contains(@id, 'INDEX_DAYLabel')]/text()")
        values = [
            tree.xpath(f"//span[@id='{span_id.replace('DAY', 'CLOSE')}']/text()")[0].strip()
            for span_id in tree.xpath("//span[contains(@id, 'INDEX_DAYLabel')]/@id")
        ]

        return list(zip(dates, values))


    async def clear_downloads_directory(self,download_path):
        # Clear the download directory
        if os.path.exists(download_path):
            for file in os.listdir(download_path):
                file_path = os.path.join(download_path, file)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove the file or link
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove directory
                        log.info(f"Deleted {file_path}")
                except Exception as e:
                    log.error(f"Failed to delete {file_path}. Reason: {e}")


    async def download_file(self):
        download_path = os.path.join(os.getcwd(), 'downloads')
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        # Clear the downloads directory first
        await self.clear_downloads_directory(download_path)

        # Set up download behavior to monitor downloads
        await self.page._client.send('Page.setDownloadBehavior', {
            'behavior': 'allow',
            'downloadPath': download_path
        })

        await self.page.goto(self.url)
        log.info('Page loaded [green]successfully[/green].')
        await asyncio.sleep(1)
        # Trigger the file download
        download_link_selector = '#ctl00_C_I_LinkButton2'
        await self.page.waitForSelector(download_link_selector, timeout=10000)
        await self.page.evaluate(f'''document.querySelector("{download_link_selector}").click();''')
        log.info('Download [green]initiated[/green].')
        time.sleep(2)
        # Monitor download progress
        start_time = time.time()
        downloaded_file = None

        while True:
            files = os.listdir(download_path)
            if files:
                for file in files:
                    if file.endswith('.crdownload'):
                        log.info('--> File is still downloading.')
                        time.sleep(1)
                        break
                else:
                    # Handle both .crdownload and .xls cases
                    for file in files:
                        if file.endswith('.crdownload') or file.endswith('.xls'):
                            # Generate a new name based on the current timestamp
                            new_file_name = f"EGXMC.xls"
                            new_file_path = os.path.join(download_path, new_file_name)
                            
                            original_file_path = os.path.join(download_path, file)
                            shutil.move(original_file_path, new_file_path)
                            
                            downloaded_file = new_file_name
                            log.info(f"Download [green]completed[/green] and file renamed to: [underline blue]{new_file_name}[/underline blue].")
                            break
                    if downloaded_file:
                        break

            if time.time() - start_time > 120:  # Timeout after 2 minutes
                log.warning('Download did not complete within the expected time.')
                break
            await asyncio.sleep(1)  # Check every second

        return downloaded_file  # Return the downloaded file name

    async def close_browser(self):
        if self.browser:
            await self.browser.close()
            log.info('Browser [green]closed[/green].')

class MarketCapitalProcessor:
    def __init__(self, data):
        self.data = data

    def process_latest_data(self):
        if self.data:
            self.data.sort(key=lambda x: tuple(map(int, x[0].split('/')[::-1])))
            latest_date, latest_value = self.data[-1]
            log.info(f"Latest Date: {latest_date}, Value: {latest_value}")
            return latest_date, latest_value
        else:
            log.info('No data found.')
            return None, None

    def save_to_database(self, date, value):
        insert_data_into_db(date, value)
        

    def save_to_excel(self, date, value):
        insert_data_into_excel(date, value)
        

# async def main():
#     # Define constants
#     script_dir = os.path.dirname(os.path.abspath(__file__))  
#     url = 'https://www.egx.com.eg/ar/MCData.aspx'
#     browser_path = os.path.join(script_dir, 'Chromium\\Application\\chrome.exe')
    
#     # Initialize the scraper
#     scraper = MarketCapitalScraper(url, browser_path)
    
#     try:
#         await scraper.setup_browser()
#         # Download the file
#         downloaded_file = await scraper.download_file()
#         log.info(f'Downloaded file: {downloaded_file}')

#         await scraper.navigate_to_last_page()
#         data = await scraper.extract_data()
        
#         # Process the data
#         processor = MarketCapitalProcessor(data)
#         latest_date, latest_value = processor.process_latest_data()
        
#         if latest_date and latest_value:
#             processor.save_to_database(latest_date, latest_value)
#             processor.save_to_excel(latest_date, latest_value)
#         else:
#             print('No data found.')
        
#     except Exception as e:
#         log.error(f"An error occurred: {e}")
    
#     finally:
#         await scraper.close_browser()

# # Run the main function
# asyncio.get_event_loop().run_until_complete(main())