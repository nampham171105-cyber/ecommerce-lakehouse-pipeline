COPY users FROM '/tmp/dataset/users.csv' DELIMITER ',' CSV HEADER;

COPY products FROM '/tmp/dataset/products.csv' DELIMITER ',' CSV HEADER;

COPY sessions FROM '/tmp/dataset/sessions.csv' DELIMITER ',' CSV HEADER;

COPY interactions FROM '/tmp/dataset/interactions.csv' DELIMITER ',' CSV HEADER;

COPY purchases FROM '/tmp/dataset/purchases.csv' DELIMITER ',' CSV HEADER;

COPY reviews FROM '/tmp/dataset/reviews.csv' DELIMITER ',' CSV HEADER;