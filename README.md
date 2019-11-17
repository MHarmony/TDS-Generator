# TDS Generator

Connects to an existing PostgreSQL database, creates Tableau Data Source (TDS) files from tables or a custom SQL string, and publishes the TDS files to an existing Tableau Server.

## Getting Started

1. Clone the repository
2. Edit [config.ini](config.ini) with your environment-specific variables
3. Install the package requirements using `pip install -r requirements.txt`
3. Run the python file!

### Prerequisites

* Python - Developed and tested on version [3.7.5](https://www.python.org/downloads/release/python-375/)
* Access to a running Tableau Server - Developed and tested on version [2019.1.6](https://www.tableau.com/support/releases/server/2019.1.6)
* Access to a running PostgreSQL Server - Developed and tested on version [10.X](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/MHarmony/eaffc76b00fe76599135951d4ba9c07b) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

* **Michael Harmon** - [GitHub](https://github.com/MHarmony)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
