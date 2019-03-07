# CrowdfundingScraper
Contains scripts to aid in downloading Kickstarter and Kiva data to local databases (kickstarter.db and kiva.db).

# Updating the databases
Simply run [main.py](main.py).

# New setup
1. Be sure to call [create_new_databases()](main.py).
2. It is advisable to get as many project ids/urls as possible from webrobots.io. [webrobots_download.py](kickstarter/webrobots_download.py) can help with this.
3. Call [add_old_projects_to_all_files()](kickstarter/kickstarter_updater.py)
4. Run [main.py](main.py).

# Environment Setup
1. conda install requests regex tzlocal cython joblib lxml pandas selenium sqlite
2. pip install CurrencyConverter dateparser fake-useragent