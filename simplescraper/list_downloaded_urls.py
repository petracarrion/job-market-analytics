from os import listdir
from os.path import isfile, join

import pandas as pd

PREFIX = 'https://www.stepstone.de/'
DIR_PATH = './data/cache/www.stepstone.de'

urls = [PREFIX + f for f in listdir(DIR_PATH) if isfile(join(DIR_PATH, f))]
print(urls)
df = pd.DataFrame(urls, columns=['job_url'])
df.to_csv('./data/results/downloaded_urls.csv', index=False)
