import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
from io import BytesIO
from datetime import datetime, date
from telegram import Bot

# Constants
GITHUB_REPO = 'https://github.com/MoH-Malaysia/data-darah-public'
GITHUB_RAW_URL = f'{GITHUB_REPO}/raw/main'
DATA_FILES = [
    'donations_facility.csv',
    'donations_state.csv',
    'newdonors_facility.csv',
    'newdonors_state.csv'
]
PARQUET_URL = 'https://dub.sh/ds-data-granular'

TELEGRAM_BOT_TOKEN = 'xxxxxxx'
TELEGRAM_GROUP_CHAT_ID = 'xxxxxxx'

# Download data from GitHub
def download_data(file):
    url = f'{GITHUB_RAW_URL}/{file}'
    response = requests.get(url)
    if response.status_code == 200:
        return pd.read_csv(BytesIO(response.content))
    else:
        print(f"Failed to download {file} from GitHub.")
        return None

# Download parquet data
def download_parquet_data():
    return pd.read_parquet(PARQUET_URL)

# Generate analysis
def generate_analysis(donations_by_state, granular_df, newdonors_by_state):
    # Data Processing
    donations_by_state['date'] = pd.to_datetime(donations_by_state['date'])
    donations_by_state['year'] = donations_by_state['date'].dt.year

    # YoY Trend
    donations_by_year = donations_by_state.groupby(['year', 'state'])[['daily']].sum().reset_index()
    curr_year = pd.to_datetime("now").year
    MY_yoy_data = donations_by_year[(donations_by_year['year'] != curr_year) & (donations_by_year['state'] == "Malaysia")]
    MY_yoy_data = MY_yoy_data.drop(columns='state')
    MY_yoy_data.plot(x='year', y='daily', legend=False)
    plt.xlabel("Year")
    plt.ylabel("Total Blood Donation")
    x = MY_yoy_data['year'].unique()
    plt.title("YoY Trend Blood Donotion in Malaysia")
    plt.xticks(x, rotation=45)
    plt.savefig('yoy_trend.png')
    plt.close()

    # Daily Trend
    today = pd.to_datetime("now")
    MY_daily_data = donations_by_state[((pd.to_datetime("now") - donations_by_state['date'])/ np.timedelta64(1, 'D') <= 31) & (donations_by_state['state'] == "Malaysia")]
    MY_daily_data = MY_daily_data.groupby(['date', 'state'])[['daily']].sum().reset_index()
    MY_daily_data = MY_daily_data.drop(columns='state')
    MY_daily_data.plot(x='date', y='daily', legend=False)
    plt.xlabel("Date")
    plt.ylabel("Total Blood Donation")
    plt.title("Last 30 Days Trend Blood Donotion in Malaysia")
    plt.savefig('daily_trend.png')
    plt.close()

    # Yearly Retention
    granular_df['visit_date'] = pd.to_datetime(granular_df['visit_date'])
    granular_df['visit_year'] = granular_df['visit_date'].dt.year
    yearly_visit = granular_df.groupby(['visit_year', 'donor_id'])[['visit_date']].nunique().reset_index()
    yearly_tot_donor = yearly_visit.groupby(['visit_year'])[['donor_id']].nunique().reset_index()
    yearly_rep_donor = yearly_visit[yearly_visit['visit_date'] > 1][['visit_year', 'donor_id']]
    yearly_rep_donor = yearly_rep_donor.groupby(['visit_year'])[['donor_id']].nunique().reset_index()
    yearly_tot_donor = yearly_tot_donor.rename(columns = {'donor_id':'total_donors'})
    yearly_rep_donor = yearly_rep_donor.rename(columns = {'donor_id':'rep_donors'})
    yearly_retention = pd.merge(yearly_tot_donor, yearly_rep_donor, on='visit_year')
    yearly_retention['retention_rate'] = yearly_retention['rep_donors'] / yearly_retention['total_donors'] * 100
    yearly_retention = yearly_retention[(yearly_retention['visit_year'] != curr_year)]
    yearly_retention.plot(x='visit_year', y='retention_rate', legend=False)
    plt.xlabel("Year")
    plt.ylabel("Retention Rate (%)")
    x = yearly_retention['visit_year'].unique()
    plt.title("Blood Donors Yearly Retention Rate")
    plt.xticks(x, rotation=45)
    plt.savefig('retention_year.png')
    plt.close()

    # New vs Existing
    newdonors_by_state['date'] = pd.to_datetime(newdonors_by_state['date'])
    new_vs_existing = pd.merge(donations_by_state[['date', 'year', 'state', 'daily']], newdonors_by_state[['date', 'state', 'total']], on=['date', 'state'])
    new_vs_existing = new_vs_existing.rename(columns = {'daily':'total',
                                                    'total':'new'})
    new_vs_existing['existing'] = new_vs_existing['total'] - new_vs_existing['new']
    new_vs_existing[new_vs_existing['state'] == "Malaysia"]
    yearly_nve_split = new_vs_existing.groupby(['year'])[['new', 'existing', 'total']].sum().reset_index()
    yearly_nve_split['% new'] = yearly_nve_split['new'] / yearly_nve_split['total'] * 100
    yearly_nve_split['% existing'] = yearly_nve_split['existing'] / yearly_nve_split['total'] * 100
    plt.bar(yearly_nve_split['year'], yearly_nve_split['% existing'], label='Existing')
    plt.bar(yearly_nve_split['year'], yearly_nve_split['% new'], bottom=yearly_nve_split['% existing'], label='New')
    plt.xlabel("Year")
    plt.ylabel("% Total Blood Donation")
    x = yearly_nve_split['year'].unique()
    plt.legend(["Existing", "New"], loc=2, ncol=2)
    plt.title("% Total Blood Donation by Donor Status")
    plt.xticks(x, rotation=45)
    plt.savefig('nve.png')
    plt.close()

# Send results to Telegram group
def send_results_to_telegram():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    bot.send_message(chat_id=TELEGRAM_GROUP_CHAT_ID, text='Blood Donation Analysis')

    # Send visualizations
    bot.send_photo(chat_id=TELEGRAM_GROUP_CHAT_ID, photo=open('yoy_trend.png', 'rb'))
    bot.send_photo(chat_id=TELEGRAM_GROUP_CHAT_ID, photo=open('daily_trend.png', 'rb'))
    bot.send_photo(chat_id=TELEGRAM_GROUP_CHAT_ID, photo=open('retention_year.png', 'rb'))
    bot.send_photo(chat_id=TELEGRAM_GROUP_CHAT_ID, photo=open('nve.png', 'rb'))

if __name__ == "__main__":
    donations_by_state = download_data('donations_state.csv')
    newdonors_by_state = download_data('newdonors_state.csv')
    granular_df = download_parquet_data()

    # Perform analysis and generate visualizations
    generate_analysis(donations_by_state, granular_df, newdonors_by_state)

    # Send results to Telegram group
    send_results_to_telegram()
