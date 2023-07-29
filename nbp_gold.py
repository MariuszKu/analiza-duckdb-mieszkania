from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 
from datetime import datetime, timedelta

def last_day_of_month(date):
    next_month = date.replace(day=28) + timedelta(days=4)  # Move to the next month
    return next_month - timedelta(days=next_month.day)

def generate_last_days_of_months(start_date, end_date):
    date_list = []
    current_date = datetime.strptime(start_date, "%d-%m-%Y")
    end_date = datetime.strptime(end_date, "%d-%m-%Y")

    while current_date <= end_date:
        last_day = last_day_of_month(current_date) - timedelta(days=1)
        date_list.append(last_day.strftime("%Y-%m-%d"))
        last_day = last_day_of_month(current_date) - timedelta(days=2)
        date_list.append(last_day.strftime("%Y-%m-%d"))
        last_day = last_day_of_month(current_date)
        date_list.append(last_day.strftime("%Y-%m-%d"))
        current_date = last_day + timedelta(days=1)

    return date_list


start_date = "01-01-2007"
end_date = "31-12-2012"
last_days_of_months = generate_last_days_of_months(start_date, end_date)

# instantiate options 
options = webdriver.ChromeOptions() 
 
# run browser in headless mode 
options.headless = True 
 
# instantiate driver 
driver = webdriver.Chrome(service=ChromeService( 
	ChromeDriverManager().install()), options=options) 
 
with open("data/gold.csv","a+") as file:
    for last_day in last_days_of_months:
        url = f'https://nbp.pl/cena-zlota-archiwum/cena-zlota-z-dnia-{last_day}/' 
        
        # get the entire website content 
        driver.get(url) 
        
        # select elements by class name 
        elements = driver.find_elements(By.CLASS_NAME, 'section__single') 
        for title in elements: 
            # select H2s, within element, by tag name 
            #print(title)
            #heading = title.find_element(By.TAG_NAME, 'td').text 
            heading = title.find_element(By.TAG_NAME, 'tbody').text 
            # print H2s 
            heading = heading.replace(" ",",")
            file.write(f"{heading}\n")
            print(heading)