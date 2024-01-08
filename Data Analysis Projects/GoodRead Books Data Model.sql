/*
DROP TABLE If EXISTS myschema.Books ;
DROP TABLE IF EXISTS myschema.Publisher;


CREATE TABLE myschema.Books (
	book_ID VARCHAR(10), 
	book_name VARCHAR(255),
	ISN VARCHAR(15) , 
	page_number INT, 
	count_of_reviews INT,
	language VARCHAR(5),
	Rating float,
	Publisher_name VARCHAR(255)
);

CREATE TABLE myschema.Publisher(
	publisher_ID VARCHAR(10),
	publisher_name VARCHAR(255)

);


DROP TABLE If EXISTS myschema.Author ;
DROP TABLE IF EXISTS myschema.Reviews;

CREATE TABLE myschema.Author(
	author_ID VARCHAR(10),	
	author_name VARCHAR(255)

);

CREATE TABLE myschema.Reviews(
	User_ID VARCHAR(10)	,
	book_name VARCHAR(255),
	user_comment VARCHAR(255)
);

*/




