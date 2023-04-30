--1- write a query to make sure that the customer cannot book an occupied room


CREATE OR REPLACE TRIGGER CHECK_AVAIL
BEFORE INSERT 
ON RESERVATION
FOR EACH ROW
DECLARE

V_STATUS_ROOM VARCHAR2(10);

BEGIN 
SELECT 
OCCUPIED INTO V_STATUS_ROOM
FROM ROOM
WHERE ROOM_ID = :NEW.ROOM_ID;

IF V_STATUS_ROOM = 'Yes' THEN

DBMS_OUTPUT.PUT_LINE('The room is not available');

END IF;

END;



----------------------------------------------

--2- write a query to update the reservation status (availablity for the rooms)


CREATE OR REPLACE TRIGGER ROOM_STATUS_CHANGE
AFTER INSERT OR UPDATE OR DELETE
ON RESERVATION 
FOR EACH ROW
BEGIN 

IF INSERTING THEN

UPDATE ROOM
SET OCCUPIED = 'Yes'
WHERE ROOM_ID = :NEW.ROOM_ID;

ELSIF DELETING THEN

UPDATE ROOM
SET OCCUPIED = 'No'
WHERE ROOM_ID = :OLD.ROOM_ID;

ELSIF UPDATING THEN

UPDATE ROOM
SET OCCUPIED = 'Yes'
WHERE ROOM_ID = :NEW.ROOM_ID;

UPDATE ROOM
SET OCCUPIED = 'NO'
WHERE ROOM_ID = :OLD.ROOM_ID;


END IF;

END;


----------------------------------------------

--3- write a query to count the total number of customers who booked a single room with sea option



SELECT 
CUST.NATIONAL_ID,
RO.ROOM_TYPE,
RES.SEA_OPTION
FROM 
CUSTOMER CUST,
RESERVATION RES,
ROOM RO
WHERE RO.ROOM_ID = RES.ROOM_ID
AND CUST.CUSTOMER_ID = RES.CUSTOMER_ID
AND RES.SEA_OPTION = 'Yes'
AND RO.RoOM_TYPE = 'Single';






----------------------------------------------


--4- write a query to list all the reservations that ends in  July 2006
--including the customer's name, reservation dates and room(s) numbers


SELECT 
CONCAT(CUST.FIRST_NAME,CONCAT(' ',CUST.LAST_NAME)) "CUSTOMER NAME",
RES.RESERVATION_DATE,
RES.CHECK_OUT,
RO.ROOM_ID
FROM 
ROOM RO INNER JOIN RESERVATION RES ON
RO.ROOM_ID = RES.ROOM_ID INNER JOIN CUSTOMER CUST
ON CUST.CUSTOMER_ID = RES.CUSTOMER_ID
WHERE TO_CHAR(RES.CHECK_OUT,'mm/yyyy') = '07/2006';



----------------------------------------------

--5- write a query to return a list of all reservations for rooms with SPA body mask
--service,displaying the customer's name,the reservation_id , the room number and the reservation date.


SELECT DISTINCT
CUST.FIRST_NAME,
CUST.LAST_NAME,
CUST.CUSTOMER_ID,
RO.ROOM_ID,
RES.RESERVATION_DATE,
SER.SERVICE_TYPE,
SER.SERVICE_NAME
FROM 
ROOM RO,
RESERVATION RES,
CUSTOMER CUST,
SERVICE SER,
SER_RES SR
WHERE RO.ROOM_ID = RES.ROOM_ID
AND RES.CUSTOMER_ID = CUST.CUSTOMER_ID
AND SER.SERVICE_ID = SR.SERVICE_ID
AND RES.RESERVATION_ID = SR.RESERVATION_ID
AND SER.SERVICE_NAME = 'Body_mask';


----------------------------------------------


--6- write a query to return a list of all rooms reserved for the customer whose national_id= 2000570
--including the customer's name,the rooms reserved and the starting date to the registration

SELECT 
CUST.FIRST_NAME || ' ' ||CUST.LAST_NAME AS FULL_NAME,
RO.ROOM_ID,
RES.CHECK_IN
FROM
CUSTOMER CUST,
RESERVATION RES,
ROOM RO 
WHERE CUST.CUSTOMER_ID = RES.CUSTOMER_ID
AND RO.ROOM_ID = RES.ROOM_ID
AND CUST.NATIONAL_ID =  2000570;


----------------------------------------------

--7- write a query to return a list of all customer names and the number of reservations
--sorted starting with the customer with the most reservations and then by the customer's last name
--the answer is not correct


SELECT 
CUST.FIRST_NAME,
CUST.LAST_NAME,
COUNT(RES.RESERVATION_ID) AS NUMBER_RESERVATIONS
FROM 
RESERVATION RES,
CUSTOMER CUST
WHERE CUST.CUSTOMER_ID = RES.CUSTOMER_ID
GROUP BY CUST.FIRST_NAME,CUST.LAST_NAME
ORDER BY NUMBER_RESERVATIONS DESC,CUST.LAST_NAME ASC;



----------------------------------------------

--8- write a query to display all unreserved rooms order by number of beds in descending order

SELECT 
*
FROM 
ROOM 
WHERE OCCUPIED = 'No'
ORDER BY NUMBER_OF_BEDS DESC;


----------------------------------------------

--9- write a query to list all double and family rooms with a price below 130

SELECT 
RO.ROOM_TYPE,
RES.ROOM_PRICE
FROM 
RESERVATION RES INNER JOIN ROOM RO
ON RES.ROOM_ID = RO.ROOM_ID
WHERE RES.ROOM_PRICE < 130
AND RO.ROOM_TYPE IN ('Double','Family');


----------------------------------------------

--10-write a query to list the details of all rooms,including the name of the customer
--staying in the room, if the room is occupied

SELECT * FROM ROOM;
SELECT 
RO.ROOM_ID,
RO.ROOM_TYPE,
RO.NUMBER_OF_BEDS,
RO.OCCUPIED,
RO.HOTEL_ID,
CUST.FIRST_NAME,
CUST.LAST_NAME,
RES.RESERVATION_DATE
FROM 
ROOM RO,
RESERVATION RES,
CUSTOMER CUST
WHERE RO.ROOM_ID = RES.ROOM_ID
AND RES.CUSTOMER_ID = CUST.CUSTOMER_ID
AND RO.OCCUPIED = 'Yes';



----------------------------------------------

--11- write a query to list all spa services with name ,gender and phone number of registered customers

SELECT 
SER.SERVICE_TYPE,
SER.SERVICE_NAME,
CUST.GENDER,
CUST.PHONE_NUMBER
FROM 
CUSTOMER CUST,
RESERVATION RES,
SER_RES SR,
SERVICE SER
WHERE 
CUST.CUSTOMER_ID = RES.CUSTOMER_ID
AND SER.SERVICE_ID = SR.SERVICE_ID
AND RES.RESERVATION_ID = SR.RESERVATION_ID
AND SER.SERVICE_TYPE = 'SPA';







----------------------------------------------

--12- write a query to compute the total revenue per night for all double rooms

SELECT 
SUM(ROOM_PRICE) AS REVENUE_DOUBLE
FROM 
ROOM RO INNER JOIN RESERVATION RES
ON RO.ROOM_ID = RES.ROOM_ID
WHERE RO.ROOM_TYPE = 'Double';




----------------------------------------------

--13- write a query to display the name,address and the phone number of a customer
--whose phone number like 77945

SELECT 
FIRST_NAME,
LAST_NAME,
ADDRESS,
PHONE_NUMBER
FROM 
CUSTOMER 
WHERE PHONE_NUMBER LIKE '77945';

----------------------------------------------


--14- write a query to list all the rooms which have at most 3 beds and reserved on any date in April 2002

SELECT 
RO.ROOM_ID,
RO.NUMBER_OF_BEDS,
RES.RESERVATION_DATE
FROM
ROOM RO INNER JOIN RESERVATION RES ON
RO.ROOM_ID = RES.ROOM_ID
WHERE RO.NUMBER_OF_BEDS <= 3 
AND TO_CHAR(RES.RESERVATION_DATE,'mm/yyyy') = '04/2002';



----------------------------------------------

--15- create a trigger to make sure that all customers are greater than 18 years old


CREATE OR REPLACE TRIGGER CHECK_AGE_NEW
BEFORE INSERT
ON CUSTOMER
FOR EACH ROW
BEGIN

IF TRUNC((SYSDATE - :NEW.BIRTHDATE)/365) < 18  THEN
DBMS_OUTPUT.PUT_LINE('The age must be 18 years old or greater');

end if;

END;


-------------------------------------------------

--15- write a query to create an audit table to save any operation on the reservation's table,
--including the user's name, operation's date,operation's name and the reservation id

CREATE TABLE AUDIT_RESERVATION
(
RESERVATION_ID NUMBER,
OPERATION_NAME VARCHAR2(100),
OPERATION_DATE DATE,
BY_USER VARCHAR2(100)
);

CREATE OR REPLACE TRIGGER AUDIT_RES
AFTER INSERT OR DELETE OR UPDATE 
ON RESERVATION
FOR EACH ROW

BEGIN 

IF INSERTING THEN 

INSERT INTO AUDIT_RESERVATION VALUES(:NEW.RESERVATION_ID,'Insertion',SYSDATE,USER);

END IF;

IF DELETING THEN

INSERT INTO AUDIT_RESERVATION VALUES(:OLD.RESERVATION_ID,'Deletion',SYSDATE,USER);

END IF;

IF UPDATING THEN 

INSERT INTO AUDIT_RESERVATION VALUES(:OLD.RESERVATION_ID,'Update',SYSDATE,USER);
END IF;
END;



-------------------------------------------------------


--16- write a query to find out the total cost for each reservation


SELECT 
SR.RESERVATION_ID,
SR.SERVICE_ID,
SER.PRICE
FROM 
SER_RES SR,
SERVICE SER
WHERE SER.SERVICE_ID = SR.SERVICE_ID;

CREATE OR REPLACE PROCEDURE UPDATE_TOTAL_COST(P_RESERVATION_ID RESERVATION.RESERVATION_ID %TYPE)
IS
BEGIN

UPDATE RESERVATION
SET TOTAL_COST = ROOM_PRICE + 
(SELECT SUM(SER.PRICE)
FROM SER_RES SR,
SERVICE SER
WHERE SER.SERVICE_ID = SR.SERVICE_ID
AND RESERVATION_ID = P_RESERVATION_ID
GROUP BY RESERVATION_ID)
WHERE RESERVATION_ID = P_RESERVATION_ID;

END;



--to find the total cost for each reservation

DECLARE
V_RESERVATION_ID NUMBER := 3;

BEGIN

LOOP

UPDATE_TOTAL_COST(V_RESERVATION_ID);

V_RESERVATION_ID := V_RESERVATION_ID + 1;

EXIT WHEN V_RESERVATION_ID = 50;

END LOOP;

END;

SELECT * FROM RESERVATION;


--------------------------------------------------

--17- Write a query to find the total number of reservations for each region

SELECT 
REG.REGION_NAME,
NVL(COUNT(RES.RESERVATION_ID),0) AS TOTAL_RESERVATIONS
FROM 
REGION REG,
COUNTRY COUN,
LOCATION LOC,
HOTEL HT,
ROOM RO,
RESERVATION RES
WHERE REG.REGION_ID = COUN.REGION_ID(+)
AND COUN.COUNTRY_ID = LOC.COUNTRY_ID(+)
AND LOC.LOCATION_ID = HT.LOCATION_ID(+)
AND HT.HOTEL_ID = RO.HOTEL_ID(+)
AND RO.ROOM_ID = RES.ROOM_ID(+)
GROUP BY REG.REGION_NAME;



