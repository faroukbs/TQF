# TQF 

Project Description:

This project fetches and processes data from the open-source dataset provided by [Acheter à Paris - Commerçants parisiens - Retrait de commande ou livraison](https://opendata.paris.fr/pages/home/), which contains information about businesses in Paris that offer order pickup or delivery services. The data is extracted, transformed, and loaded into a database for further analysis.

Key Features and Highlights:

- Data Extraction: The project fetches data from the open data portal, ensuring that it's up to date and accurate.

- Data Transformation: The dataset is transformed to extract relevant information, including location data, to make it suitable for analysis.

- Database Integration: The transformed data is loaded into a MySQL database for easy retrieval and querying.

- Data Visualization: The project includes data visualization using Tableau to provide insights and trends related to the businesses in Paris offering pickup and delivery services.

- Static Web Page: A static web page is generated and hosted on GitHub to showcase the visualizations and provide an accessible interface for users to explore the data.

Why It's Useful:

This project serves as a valuable resource for anyone interested in understanding the landscape of businesses in Paris that offer order pickup or delivery services. It provides insights into the types of businesses, their locations, and other relevant details.

Who Might Be Interested:

- Data Analysts: Professionals looking to analyze and extract insights from the dataset.
- Business Owners: Business owners or entrepreneurs interested in market research in the Paris area.
- Open Data Enthusiasts: Individuals passionate about working with open data to drive insights and innovation.

## Table of Contents
- [Local Installation](#local-installation)
- [Docker Installation](#docker-installation)
- [Data Visualization](#Data-Visualization)


## Local Installation

To run this project locally, follow these steps:

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/faroukbs/TQF.git
Install Python:

If you don't have Python installed, you can download it frome[here](https://www.python.org/downloads/) and follow the installation instructions for your platform.

Install Dependencies:

Open a terminal and navigate to the project directory, then run:
1. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
This will install the necessary libraries:

requests
pandas
SQLAlchemy
pymysql
Run the Project:

Open a terminal, navigate to the project directory, and run:

3. **Run the project (local)**

   ```bash
   python test.py

## Docker Installation
If you prefer to run the project using Docker, follow these steps:

Install Docker:

If you don't have Docker installed, you can download it from [Docker's official website](https://docs.docker.com/engine/install/) and follow the installation instructions for your platform.

Clone the Repository:

3. **clone teh project**

   ```bash
   git clone https://github.com/faroukbs/TQF.git
Build and Run the Docker Containers:

Open a terminal, navigate to the project directory, and run:

3. **Run the project in a container**

   ```bash
   docker-compose up
This command will build and start the necessary Docker containers for your project.



## Data Visualization
To view the data visualization, visit this link [Data Visualization](https://roky-dev.github.io/roky-dev/#).
