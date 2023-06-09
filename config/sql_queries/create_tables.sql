CREATE TABLE Date_Dimension (
    Date_Key SERIAL PRIMARY KEY,
    Incident_Datetime TIMESTAMP NOT NULL,
    Incident_Date DATE NOT NULL,
    Incident_Time TIME NOT NULL,
    Incident_Year INT NOT NULL,
    Incident_Day_of_Week VARCHAR(255) NOT NULL,
    Report_Datetime TIMESTAMP NOT NULL
);

CREATE TABLE Category_Dimension (
    Category_Key SERIAL PRIMARY KEY,
    Incident_Category VARCHAR(255),
    Incident_Subcategory VARCHAR(255),
    Incident_Code INT NOT NULL
);

CREATE TABLE District_Dimension (
    District_Key SERIAL PRIMARY KEY,
    Police_District VARCHAR(255) NOT NULL,
    Analysis_Neighborhood VARCHAR(255)
);

CREATE TABLE Resolution_Dimension (
    Resolution_Key SERIAL PRIMARY KEY,
    Resolution VARCHAR(255) NOT NULL
);

CREATE TABLE Location_Dimension (
    Location_Key SERIAL PRIMARY KEY,
    Latitude FLOAT,
    Longitude FLOAT
);

CREATE TABLE Incident_Details_Dimension (
    Incident_Details_Key SERIAL PRIMARY KEY,
    Incident_Number INT NOT NULL,
    Incident_Description TEXT
);

CREATE TABLE Incidents (
    Incident_ID BIGSERIAL PRIMARY KEY,
    Date_Key INT,
    Category_Key INT,
    District_Key INT,
    Resolution_Key INT,
    Location_Key INT,
    Incident_Details_Key INT,
    FOREIGN KEY (Date_Key) REFERENCES Date_Dimension(Date_Key),
    FOREIGN KEY (Category_Key) REFERENCES Category_Dimension(Category_Key),
    FOREIGN KEY (District_Key) REFERENCES District_Dimension(District_Key),
    FOREIGN KEY (Resolution_Key) REFERENCES Resolution_Dimension(Resolution_Key),
    FOREIGN KEY (Location_Key) REFERENCES Location_Dimension(Location_Key),
    FOREIGN KEY (Incident_Details_Key) REFERENCES Incident_Details_Dimension(Incident_Details_Key)
);