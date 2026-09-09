CREATE TABLE users (
    user_id varchar(100) primary key,
    age int,
    gender varchar(20),
    country varchar(20),
    city varchar(50),
    signup_date date,
    income_level varchar(50),
    preferred_category varchar(50),
    loyalty_tier varchar(50)
);

CREATE TABLE products (
    product_id varchar(100) primary key,
    product_name varchar(100),
    product_description text,
    category varchar(50),
    subcategory varchar(50),
    brand varchar(50),
    price decimal(18,2),
    rating_avg float,
    review_count int,
    stock_quantity int,
    date_added date
);

CREATE TABLE sessions (
    session_id varchar(100) primary key,
    user_id varchar(100) references users(user_id),
    start_time timestamp,
    device_type varchar(50),
    referrer_source varchar(50),
    is_converted boolean
);

CREATE TABLE interactions (
    interaction_id varchar(100) primary key,
    user_id varchar(100) references users(user_id),
    product_id varchar(100) references products(product_id),
    session_id varchar(100) references sessions(session_id),
    interaction_type varchar(50),
    interaction_timestamp timestamp,
    dwell_time_ms int
);

CREATE TABLE purchases (
    purchase_id varchar(100) primary key,
    order_id varchar(100),
    user_id varchar(100) references users(user_id),
    product_id varchar(100) references products(product_id),
    session_id varchar(100) references sessions(session_id),
    interaction_id varchar(100) references interactions(interaction_id),
    quantity int,  
    unit_price decimal(18,2),
    total_amount decimal(18,2),
    order_date timestamp
);

CREATE TABLE reviews (
    review_id varchar(100) primary key,
    user_id varchar(100) references users(user_id),
    product_id varchar(100) references products(product_id),
    purchase_id varchar(100) references purchases(purchase_id),
    rating int,
    title text,
    review_text text,
    review_date timestamp
);