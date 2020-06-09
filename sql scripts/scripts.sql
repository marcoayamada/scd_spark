CREATE TABLE dim_contact (
        sk_cliente INT AUTO_INCREMENT NOT NULL,
        nk_cliente VARCHAR(255) NOT NULL,
        nm_cliente VARCHAR(255) NOT NULL,
        nm_email VARCHAR(255) NOT NULL,
        nr_phone VARCHAR(255) NOT NULL,
        is_current BOOLEAN NOT NULL,
        is_deleted BOOLEAN NOT NULL,
        starts_at DATE NOT NULL,
        ends_at DATE NOT NULL,
        inserted_at DATETIME NOT NULL,
        PRIMARY KEY (sk_cliente)
);


CREATE TABLE st_contact (
        contact_id VARCHAR(255),
        first_name VARCHAR(255),
        middle_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(255)
);