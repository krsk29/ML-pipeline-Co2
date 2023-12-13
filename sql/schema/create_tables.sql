CREATE TABLE logistics (
    transaction_id INT,
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
