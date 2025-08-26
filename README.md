# MCP Analytics Server

A secure Python-based analytics server for processing and analyzing medical claims prescription (MCP) data. This server provides insights into prescription patterns across different medical specialties, geographical regions, and individual prescribers.

## Features

- Secure database connection handling using Azure Identity for authentication
- Comprehensive analytics on prescription data including:
  - Medical specialty statistics
  - State-level prescription analysis
  - Top prescriber insights
- Environment-based configuration
- Abstract database connection layer for enhanced security

## Prerequisites

- Python 3.x
- Azure SQL Database access
- Azure Identity credentials
- Required Python packages (see Installation)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/bob-omboto/mcp_server.git
cd mcp_server
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Configure environment variables in `.env` file:
```
AZURE_SQL_SERVER=your_server.database.windows.net
AZURE_SQL_DATABASE=your_database
```

## Usage

Run the server:
```bash
python server.py
```

The server will output analytics including:
- Detailed statistics by medical specialty
- Top 10 states by prescription claims
- Top 10 prescribers by volume

## Security Features

- Uses Azure Identity for secure authentication
- No hardcoded credentials
- Abstract database connection layer
- Secure query handling
- Environment-based configuration

## Data Insights

The server processes millions of prescription records to provide insights such as:
- Claims per specialty
- Cost analysis
- Prescriber patterns
- Geographic distribution
- Brand analysis
- Supply duration patterns

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

[MIT License](LICENSE)