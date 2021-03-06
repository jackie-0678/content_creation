CREATE TABLE offices(id int PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 MINVALUE 1 START 1),
           name text NOT NULL,
           address text NOT NULL, 
           address2 text DEFAULT NULL,
           city text NOT NULL,
           state text NOT NULL,
           zipcode text NOT NULL,
           date_added date NOT NULL DEFAULT current_timestamp);
           
INSERT INTO offices(name, address, city, state, zipcode) VALUES('Location1','123 Place Road','New York','NY','10001');
INSERT INTO offices(name, address, city, state, zipcode) VALUES('Location2','555 Rock Avenue','Boston','MA','02101');
INSERT INTO offices(name, address, city, state, zipcode) VALUES('Location3','838 Rock Avenue','Pittsburgh','PA','15106');
INSERT INTO offices(name, address, city, state, zipcode) VALUES('Location4','321 Pop Street','Philadelphia','PA','19019');
INSERT INTO offices(name, address, city, state, zipcode) VALUES('Location5','210 Windy Road','Minneapolis','MN','55111');
