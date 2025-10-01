# NextJS14-Dashboard + Data Normalization utilising Python + Terraform for IaS

A modern advocacy analytics dashboard built with Next.js that provides real-time insights into advocacy program performance across multiple social media platforms.

## 🚀 Features

- **Real-time Analytics**: Live data visualization of advocacy metrics
- **Multi-platform Support**: Track performance across Instagram, TikTok, Twitter, and YouTube
- **Interactive Dashboard**: Comprehensive metrics cards and data visualization
- **Responsive Design**: Optimized for desktop and mobile devices
- **Data Visualization**: Interactive charts powered by D3.js
- **Pagination**: Efficient data loading with pagination support
- **Type Safety**: Full TypeScript implementation
- **Automated Data Pipeline**: GitHub Actions workflow for data processing
- **Infrastructure as Code**: Terraform-managed Heroku deployment

## 🛠 Tech Stack

### Frontend
- **Next.js 14** - React framework with App Router
- **TypeScript** - Type-safe development
- **Tailwind CSS** - Utility-first CSS framework
- **Radix UI** - Accessible component primitives
- **SWR** - Data fetching with caching and revalidation
- **Axios** - HTTP client for API requests
- **D3.js** - Data visualization library
- **Lucide React** - Beautiful icon library

### Backend & Infrastructure
- **Heroku** - Cloud platform for deployment
- **PostgreSQL** - Primary database
- **Redis** - Caching and session storage
- **Terraform** - Infrastructure as Code
- **GitHub Actions** - CI/CD pipeline

### Data Processing
- **Python** - Data normalization and processing
- **Pandas** - Data manipulation and analysis
- **psycopg2** - PostgreSQL adapter for Python

## 📊 Dashboard Components

### Metrics Cards
- Total Users
- Active Programs
- Total Engagement
- Revenue Attribution

### Data Visualization
- Performance trends over time
- Platform-wise engagement metrics
- Program effectiveness charts

### Data Table
- Paginated user data
- Sortable columns
- Real-time updates

## 🚦 Getting Started

### Prerequisites
- Node.js 18+ 
- Python 3.9+
- PostgreSQL (for local development)
- Terraform (for infrastructure management)
- Heroku CLI (for deployment)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd duel
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Set up Python environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

4. **Environment Variables**
   
   Create `.env.local` file:
   ```env
   NEXT_PUBLIC_API_URL=https://duel-api-b228efdae24e.herokuapp.com
   NEXT_PUBLIC_APP_NAME=Advocacy Dashboard
   ```
   
   For data processing, create `.env` file:
   ```env
   HEROKU_DATABASE_URL=your_postgresql_connection_string
   ```

5. **Run the development server**
   ```bash
   npm run dev
   ```

   Open [http://localhost:3000](http://localhost:3000) to view the dashboard.

## 🏗 Infrastructure Management with Terraform

### Overview
The project uses Terraform to manage all Heroku infrastructure as code, ensuring reproducible and version-controlled deployments.

### Terraform Configuration (`terraform/main.tf`)

The Terraform configuration provisions:

#### Heroku Application
- **App Name**: `duel-users-api`
- **Region**: EU (Europe)
- **Stack**: `heroku-22`
- **Buildpacks**: Node.js and Python for full-stack support

#### Add-ons Provisioned
1. **PostgreSQL** (`heroku-postgresql:essential-0`)
   - Primary database for storing processed advocacy data
   - Automatic backups and high availability

2. **Redis** (`heroku-redis:mini`)
   - Caching layer for improved performance
   - Session storage and temporary data

3. **Papertrail** (`papertrail:choklad`)
   - Centralized logging and log management
   - Real-time log streaming and search

4. **New Relic** (`newrelic:wayne`)
   - Application performance monitoring (APM)
   - Error tracking and performance insights

#### Pipeline Configuration
- **Staging Environment**: For testing and validation
- **Production Environment**: Live application deployment
- **Automatic Promotion**: Configurable deployment pipeline

### Terraform Commands

```bash
# Initialize Terraform
cd terraform
terraform init

# Plan infrastructure changes
terraform plan

# Apply infrastructure changes
terraform apply

# Destroy infrastructure (use with caution)
terraform destroy
```

### Infrastructure Outputs
Terraform provides essential connection details:
- Application URL
- Database connection string
- Redis connection string

## 🔄 Data Processing Pipeline

### Overview
The data processing pipeline consists of two main Python scripts that work together to normalize, validate, and upload advocacy data from JSON files to PostgreSQL.

## 📊 Data Normalization (`normalise_data_panda.py`)

### Purpose
The `normalise_data_panda.py` script is responsible for cleaning, validating, and transforming raw JSON data from the `mixed/` directory into a standardized format suitable for database storage and dashboard consumption.

### Key Features

#### Data Validation Functions
1. **Email Validation** (`validate_email`)
   - Uses regex pattern matching for email format validation
   - Ensures data quality for user contact information

2. **Date Validation** (`validate_date`)
   - Validates and standardizes date formats
   - Handles multiple input date formats
   - Converts to ISO format for consistency

3. **URL Validation** (`validate_url`)
   - Validates social media post URLs
   - Ensures proper URL structure for tracking

4. **Social Media Handle Validation** (`validate_social_handle`)
   - Validates platform-specific username formats
   - Removes invalid characters and formatting

5. **Numeric Value Cleaning** (`clean_numeric_value`)
   - Converts string numbers to proper numeric types
   - Handles edge cases and invalid data

#### Data Processing Workflow

1. **Database Connection**
   ```python
   # Connects to PostgreSQL using HEROKU_DATABASE_URL
   conn = psycopg2.connect(database_url)
   ```

2. **Table Creation**
   - Creates `processed_data` table if it doesn't exist
   - Defines schema for normalized advocacy data

3. **File Detection Logic**
   ```python
   # Smart processing: only new files if database has data
   if database_is_empty():
       process_all_files()
   else:
       process_next_sequential_file()
   ```

4. **Data Transformation**
   - Flattens nested JSON structures
   - Normalizes user and program data
   - Aggregates task completion metrics
   - Calculates total engagement metrics

5. **Output Generation**
   - Saves processed data to `processed_data.json`
   - Maintains data lineage and traceability

### Usage
```bash
# Run data normalization
python normalise_data_panda.py

# The script will:
# 1. Check database state
# 2. Determine which files to process
# 3. Validate and clean data
# 4. Generate processed_data.json
```

## 📤 Database Upload (`uploader.py`)

### Purpose
The `uploader.py` script handles the secure and efficient upload of normalized data from `processed_data.json` to the PostgreSQL database on Heroku.

### Key Features

#### Database Management
1. **Connection Handling**
   ```python
   # Secure connection using environment variables
   conn = psycopg2.connect(os.getenv('HEROKU_DATABASE_URL'))
   ```

2. **Table Schema Management**
   ```sql
   CREATE TABLE IF NOT EXISTS processed_data (
       user_id VARCHAR(50),
       name VARCHAR(100),
       email VARCHAR(100),
       platform VARCHAR(50),
       program_id VARCHAR(50),
       brand VARCHAR(100),
       tasks_completed INTEGER,
       total_likes INTEGER,
       total_comments INTEGER,
       total_shares INTEGER,
       total_reach INTEGER,
       total_sales_attributed DECIMAL(10,2),
       joined_at TIMESTAMP
   );
   ```

#### Upload Process

1. **File Detection**
   - Scans `mixed/` directory for `user_*.json` files
   - Identifies new files not yet processed
   - Supports both incremental and full uploads

2. **Batch Processing**
   ```python
   # Efficient batch uploads for large datasets
   def upload_in_batches(data, batch_size=1000):
       for i in range(0, len(data), batch_size):
           batch = data[i:i + batch_size]
           execute_batch_insert(batch)
   ```

3. **Data Integrity**
   - Transaction-based uploads for data consistency
   - Rollback capability on errors
   - Duplicate detection and handling

4. **Progress Tracking**
   - Real-time upload progress reporting
   - Error logging and recovery
   - Upload statistics and metrics

### Upload Modes

#### Incremental Upload (Default)
```bash
# Upload only new/changed data
python3 uploader.py
```

#### Full Upload
```bash
# Upload all data (use for initial setup or data refresh)
python3 uploader.py --full
```

#### Range Upload
```bash
# Upload specific file range
python3 uploader.py --start 0 --end 50
```

### Error Handling
- **Connection Failures**: Automatic retry with exponential backoff
- **Data Validation**: Pre-upload data validation
- **Transaction Safety**: Atomic operations with rollback capability
- **Logging**: Comprehensive error logging for debugging

## 🔄 Complete Data Pipeline Workflow

### Automated Processing (GitHub Actions)
1. **Trigger**: New files added to `mixed/` directory
2. **Environment Setup**: Python dependencies installation
3. **Data Detection**: Identify new JSON files
4. **Normalization**: Run `normalise_data_panda.py`
5. **Upload**: Execute `uploader.py`
6. **Validation**: Verify data integrity

### Manual Processing
```bash
# Complete pipeline execution
# 1. Normalize data
python3 normalise_data_panda.py

# 2. Upload to database
python3 uploader.py
