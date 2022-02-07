### Not passable

import os
from time import sleep
from datetime import date, timedelta
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By as WebBy
from selenium.webdriver.support.ui import Select as WebSelect
from urllib.parse import urlsplit
from urllib.parse import quote_plus as url_quoteplus

# set up the start day
class StartDate():
    def __init__(self):
        self.today_date = date.today() - timedelta(days = 7)
        self.str_date = self.today_date.strftime('%m/%d/%Y')
        self.rep_date = self.str_date.replace('/', '%2F')

# set up the end day
class EndDate():
    def __init__(self):
        self.today_date = date.today() - timedelta(days = 1)
        self.str_end_date = self.today_date.strftime('%m/%d/%Y')
        self.rep_end_date = self.str_end_date.replace('/', '%2F')

# add argument：–allow-outdated-plugins
chrome_options = Options()
chrome_options.add_argument('--allow-outdated-plugins')
# allow using flash method
def allow_flash(driver, url):
    def _base_url(url):
        if url.find("://") == -1:
            url = "http://{}".format(url)
        urls = urlsplit(url)
        return "{}://{}".format(urls.scheme, urls.netloc)

    def _shadow_root(driver, element):
        return driver.execute_script("return arguments[0].shadowRoot", element)

    base_url = _base_url(url)
    driver.get("chrome://settings/content/siteDetails?site={}".format(url_quoteplus(base_url)))

    root1 = driver.find_element(WebBy.TAG_NAME, "settings-ui")
    shadow_root1 = _shadow_root(driver, root1)
    root2 = shadow_root1.find_element(WebBy.ID, "container")
    root3 = root2.find_element(WebBy.ID, "main")
    shadow_root3 = _shadow_root(driver, root3)
    root4 = shadow_root3.find_element(WebBy.CLASS_NAME, "showing-subpage")
    shadow_root4 = _shadow_root(driver, root4)
    root5 = shadow_root4.find_element(WebBy.ID, "advancedPage")
    root6 = root5.find_element(WebBy.TAG_NAME, "settings-privacy-page")
    shadow_root6 = _shadow_root(driver, root6)
    root7 = shadow_root6.find_element(WebBy.ID, "pages")
    root8 = root7.find_element(WebBy.TAG_NAME, "settings-subpage")
    root9 = root8.find_element(WebBy.TAG_NAME, "site-details")
    shadow_root9 = _shadow_root(driver, root9)
    root10 = shadow_root9.find_element(WebBy.ID, "plugins")  # FlaENG:CAD-COMMERCIAL_VEHICLE_Q4-XAXRTB-AUDIENCE_TARGETING-PELMOREX_DATA_P147931sh
    shadow_root10 = _shadow_root(driver, root10)
    root11 = shadow_root10.find_element(WebBy.ID, "permission")
    WebSelect(root11).select_by_value("allow")

# allow flash in Chrome
driver = webdriver.Chrome(options=chrome_options)
allow_flash(driver,'http://am.contobox.com/report/temp.html?tab=io')

driver.maximize_window()
# open the download page
driver.get('http://am.contobox.com/report/temp.html?tab=io')
driver.find_element_by_name('username').send_keys('alberto.mangones@groupm.com')
driver.find_element_by_name ('password').send_keys('amangonesCR')
driver.find_element_by_xpath('//input[@type="submit"][@value="Login"]').click()
#driver.maximize_window()
Camp_IO = driver.find_element_by_xpath( "//html/body/div[3]/div[1]/section/div[3]/div[1]/section[1]/div[1]/table[1]/tbody/tr[1]/td[2]") # find campaign I.O number
# combine the download page link
page_link = 'http://am.contobox.com/report/raw.html?database=rs&tab=summary&from=' + StartDate().rep_date + '&to=' + EndDate().rep_end_date + '&io_ids=&io_id=' + Camp_IO.text
driver.get(page_link)
sleep(2)
#save file
app_path1 = 'C:/Users/ye.lu/Documents/AutoIT/contobox-csv.exe'
os.startfile(app_path1)
# close Chrome
sleep(7)
driver.close()


