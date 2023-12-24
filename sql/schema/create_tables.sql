CREATE TABLE logistics (
    transaction_id INT PRIMARY KEY,
    project_id INT,
    material_id INT,
    transaction_date DATE,
    quantity INT,
    transport_mode VARCHAR(255),
    distance_covered FLOAT,
    CO2_emission FLOAT,
    supplier_rating FLOAT,
    project_budget FLOAT
);

CREATE TABLE materials (
    material_id INT PRIMARY KEY,
    material_name VARCHAR(255),
    material_category VARCHAR(255),
    supplier_id INT
);

CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(255),
    project_start_date DATE,
    project_end_date DATE,
    project_location VARCHAR(255),
    project_budget FLOAT
);

CREATE TABLE supplier (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_location VARCHAR(255),
    supplier_rating FLOAT
);
