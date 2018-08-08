# CrowdfundingScraper
Contains scripts to aid in downloading Kickstarter and Kiva data to a local database.

# Updating the databases
Simply run [main.py](main.py).

# New setup
1. Be sure to run [create_new_databases](main.py).
2. It is advisable to get as much as possible from webrobots.io.
3. Run [add_old_projects_to_all_files](kickstarter/kickstarter_updater.py) before calling [update](kickstarter/kickstarter_updater.py) or running [main.py](main.py).
