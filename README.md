# MCP Analytics Server

A secure Python-based REST API server for processing and analyzing medical claims prescription (MCP) data. This server provides insights into prescription patterns across different medical specialties, geographical regions, and individual prescribers through easy-to-use HTTP endpoints.

## Features

- RESTful API endpoints powered by FastAPI
- Interactive API documentation (Swagger UI and ReDoc)
- Secure database connection using Azure Identity authentication
- Comprehensive analytics on prescription data including:
  - Medical specialty statistics
  - State-level prescription analysis
  - Top prescriber insights
- Environment-based configuration
- Abstract database connection layer for enhanced security
- GitHub Actions CI/CD pipeline
- Docker-ready deployment

## Prerequisites

- Python 3.11
- Azure SQL Database instance
- Azure credentials (CLI login or other Azure authentication method)
- Git
- SQL Server ODBC Driver 18

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

3. Install dependencies:
```bash
pip install --only-binary :all: -r requirements.txt
```

4. Configure environment variables:
```
AZURE_SQL_SERVER=your_server.database.windows.net
AZURE_SQL_DATABASE=your_database
AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server
```

## API Endpoints

### 1. Root Endpoint (GET /)
- Returns a welcome message and API information
- URL: `http://localhost:8000/`

### 2. Schema Information (GET /schema)
- Returns available SQL queries and database schema
- URL: `http://localhost:8000/schema`

### 3. Prescriber Types (GET /prescriber-types)
- Lists different types of prescribers and their counts
- URL: `http://localhost:8000/prescriber-types`

### 4. Top Prescribers (GET /top-prescribers)
- Returns the most active prescribers
- Optional query parameter: `limit` (default: 10)
- URL: `http://localhost:8000/top-prescribers?limit=10`

### 5. Top States (GET /top-states)
- Provides state-wise prescription statistics
- Optional query parameter: `limit` (default: 10)
- URL: `http://localhost:8000/top-states?limit=10`

## Usage

1. Start the FastAPI server:
```bash
python api.py
```

2. Access the API documentation:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

3. Make API requests using your preferred HTTP client (curl, Postman, etc.)

## Project Structure
```
mcp_server/
├── .github/
│   └── workflows/
│       └── main.yml      # GitHub Actions workflow
├── .gitignore
├── README.md
├── api.py               # FastAPI application and endpoints
├── requirements.txt     # Python dependencies
└── server.py           # Core database functionality
```

## Security Features

- Azure Identity for secure database authentication
- No hardcoded credentials - all sensitive data in environment variables
- Abstract database connection layer
- Secure query handling
- GitHub Secrets for CI/CD configuration

## CI/CD Pipeline

The GitHub Actions workflow automatically:
1. Sets up Python 3.11
2. Installs ODBC driver
3. Installs project dependencies
4. Runs tests (when added)
5. Deploys the application

## Data Insights

The API provides rich insights into prescription data:
- Claims distribution by medical specialty
- Geographic prescription patterns
- Top prescriber analytics
- State-level statistics
- Prescriber type analysis

## Development

### Local Development
1. Clone and set up the project as described in Installation
2. Make your changes
3. Test the API endpoints locally
4. Commit your changes following conventional commit messages

### Running Tests
(Test suite to be added)

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

[MIT License](LICENSE)

## Support

For support, please open an issue in the GitHub repository.