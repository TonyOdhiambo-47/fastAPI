from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from time import sleep

# Chrome options
chrome_options = webdriver.ChromeOptions()

# Path to chromedriver executable
chrome_driver_path = r"C:\.Local Disk D\Qhala\Selenium\chromedriver.exe"
service = webdriver.chrome.service.Service(executable_path=chrome_driver_path)

# Initialize the WebDriver
driver = webdriver.Chrome(options=chrome_options, service=service)

# Open Twitter home page
driver.get("https://twitter.com")

# Wait for the page to load
wait = WebDriverWait(driver, 10)

# Click on the "Log in" link
login_link = wait.until(EC.visibility_of_element_located((By.XPATH, "//span[text()='Sign in']")))
login_link.click()

# Wait for the login page to load
email_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class, 'r-30o5oe')]")))

# Enter email address
email_input.send_keys("tonyrotich2580@gmail.com" + Keys.RETURN)

# Wait for the username input field to appear
username_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class, 'r-30o5oe')]")))

# Enter username
username_input.send_keys("mandemfamdem" + Keys.RETURN)

# Wait for the password input field to appear
password_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@type='password']")))

# Enter password
password_input.send_keys("Tonyrotich123!" + Keys.RETURN)

# Wait for the user's profile page to load
wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@data-testid='primaryColumn']")))

# Navigate to the user's profile
twitter_username = "elonmusk"
driver.get("https://twitter.com/" + twitter_username)

# Wait for the page to load
wait.until(EC.visibility_of_any_elements_located((By.XPATH, "//article[@data-testid='tweet']")))

# Scrape tweets
TimeStamps = []
Tweets = []
Likes = []
Replies = []

# Define the number of pages to scrape
num_pages = 5

# Iterate over each page
for _ in range(num_pages):
    # Find all tweet elements
    tweet_elements = driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
    
    # Iterate over each tweet element on the current page
    for tweet_element in tweet_elements:
        try:
            # Extract timestamp
            timestamp = tweet_element.find_element(By.XPATH, ".//time").get_attribute('datetime')
            TimeStamps.append(timestamp)
            
            # Extract tweet text
            tweet_text = tweet_element.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
            Tweets.append(tweet_text)
        

            
            # Extract number of likes using XPath
            like_element = tweet_element.find_element(By.XPATH, "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[3]/div/div/section/div/div/div[1]/div/div/article/div/div/div[2]/div[2]/div[4]/div/div/div[3]/button/div/div[2]/span/span/span")
            like_count = like_element.text
            Likes.append(like_count)


            
            # Extract number of replies using XPath
            reply_element = tweet_element.find_element(By.XPATH, "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[3]/div/div/section/div/div/div[1]/div/div/article/div/div/div[2]/div[2]/div[4]/div/div/div[1]/button/div/div[2]/span/span/span")
            reply_count = reply_element.text
            Replies.append(reply_count)

        except:
            pass
    
    # Scroll down to load more tweets
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(5)  # Adjust the sleep time as needed

# Create a list of dictionaries for each tweet
tweets_data = []
for timestamp, tweet, like, reply in zip(TimeStamps, Tweets, Likes, Replies):
    tweet_dict = {
        'timestamp': timestamp,
        'tweet': tweet,
        'likes': like,
        'replies': reply
    }
    tweets_data.append(tweet_dict)

# Define the output file path
json_path = r"C:\.Local Disk D\Qhala\Selenium\tweets_live2.json"

# Write the list of tweet dictionaries to a JSON file
with open(json_path, 'w') as json_file:
    json.dump(tweets_data, json_file, indent=4)

# Close the browser
driver.quit()
