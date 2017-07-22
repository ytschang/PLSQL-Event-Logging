CREATE OR REPLACE PACKAGE Pkg_Data_Type_Utl AS


    SUBTYPE ST_Boolean_Int IS PLS_INTEGER RANGE 0 .. 1;
    
    
    FUNCTION Get_Boolean_As_YN(i BOOLEAN) RETURN CHAR;
    
    FUNCTION Get_YN_As_Boolean(i CHAR) RETURN BOOLEAN;
    
    FUNCTION Get_Boolean_As_Int(i BOOLEAN) RETURN ST_Boolean_Int;
    
    FUNCTION Get_Int_As_Boolean(i INTEGER) RETURN BOOLEAN;
    
    
END Pkg_Data_Type_Utl;
/


CREATE OR REPLACE PACKAGE BODY Pkg_Data_Type_Utl AS


    FUNCTION Get_Boolean_AS_YN(i BOOLEAN) RETURN CHAR IS
    
        l_Return CHAR(1);
        
    BEGIN
    
        CASE
            WHEN i IS NULL THEN l_Return := NULL;
            WHEN i THEN l_Return := 'Y';
            ELSE l_Return := 'N';
        END CASE;
        
        RETURN l_Return;
        
    END Get_Boolean_As_YN;
    
    
    FUNCTION Get_YN_As_Boolean(i CHAR) RETURN BOOLEAN IS
    
        l_Return BOOLEAN;
        
    BEGIN
    
        CASE
            WHEN i IS NULL THEN l_Return := NULL;
            WHEN i = 'Y' THEN l_Return := TRUE;
            WHEN i = 'N' THEN l_Return := FALSE;
        END CASE;
        
        RETURN l_Return;
        
    END Get_YN_As_Boolean;
    
    
    FUNCTION Get_Boolean_AS_Int(i BOOLEAN) RETURN ST_Boolean_Int IS
    
        l_Return ST_Boolean_Int;
        
    BEGIN
    
        CASE
            WHEN i IS NULL THEN l_Return := NULL;
            WHEN i THEN l_Return := 1;
            ELSE l_Return := 0;
        END CASE;
        
        RETURN l_Return;
        
    END Get_Boolean_As_Int;
    
    
    FUNCTION Get_Int_As_Boolean(i INTEGER) RETURN BOOLEAN IS
    
        l_Return BOOLEAN;
        
    BEGIN
    
        CASE
            WHEN i IS NULL THEN l_Return := NULL;
            WHEN i = 1 THEN l_Return := TRUE;
            WHEN i = 0 THEN l_Return := FALSE;
        END CASE;
        
        RETURN l_Return;
        
    END Get_Int_As_Boolean;
    
    
END Pkg_Data_Type_Utl;
/

GRANT EXECUTE ON Pkg_Data_Type_Utl TO PUBLIC
/

CREATE OR REPLACE PUBLIC SYNONYM Pkg_Data_Type_Utl FOR Pkg_Data_Type_Utl
/


CREATE TABLE A_PLS_Event_Types (
  Action_Cd CHAR(1) NOT NULL CHECK (Action_Cd IN ('I', 'U', 'D')),
  TS TIMESTAMP NOT NULL,
  Session_User VARCHAR2(30) NOT NULL,
  OS_User VARCHAR2(30) NOT NULL,
  Terminal VARCHAR2(30),
  Module VARCHAR2(48),
  Client_Id VARCHAR2(30),
  PLS_Event_Type_Cd VARCHAR2(10) NOT NULL,
  Descr VARCHAR2(100)
)
/


CREATE OR REPLACE TRIGGER TC_A_PLS_Event_Types FOR INSERT ON A_PLS_Event_Types COMPOUND TRIGGER
    BEFORE EACH ROW IS
    BEGIN
      :NEW.TS := SYSTIMESTAMP;
      :NEW.Session_User := SYS_CONTEXT('USERENV', 'SESSION_USER');
      :NEW.OS_User := SYS_CONTEXT('USERENV', 'OS_USER');
      :NEW.Terminal := SYS_CONTEXT('USERENV', 'TERMINAL');
      :NEW.Module := SYS_CONTEXT('USERENV', 'MODULE');
      :NEW.Client_Id := SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER');
    END BEFORE EACH ROW;
END TC_A_PLS_Event_Types;
/


CREATE TABLE PLS_Event_Types (
  PLS_Event_Type_Cd VARCHAR2(10) PRIMARY KEY,
  Descr VARCHAR2(100)
) ORGANIZATION INDEX
/

CREATE OR REPLACE TRIGGER TC_PLS_Event_Types FOR INSERT OR UPDATE OR DELETE ON PLS_Event_Types COMPOUND TRIGGER
    AFTER EACH ROW IS
    BEGIN
        CASE
            WHEN INSERTING THEN
                INSERT INTO A_PLS_Event_Types (Action_Cd, PLS_Event_Type_Cd, Descr)
                VALUES ('I', :NEW.PLS_Event_Type_Cd, :NEW.Descr);
            WHEN UPDATING THEN
                INSERT INTO A_PLS_Event_Types (Action_Cd, PLS_Event_Type_Cd, Descr)
                VALUES ('U', :NEW.PLS_Event_Type_Cd, :NEW.Descr);
            WHEN DELETING THEN
                INSERT INTO A_PLS_Event_Types (Action_Cd, PLS_Event_Type_Cd) VALUES ('D', :OLD.PLS_Event_Type_Cd);
        END CASE;
    END AFTER EACH ROW;
END TC_PLS_Event_Types;
/

INSERT INTO PLS_Event_Types VALUES ('Exec', 'Program unit executed');
INSERT INTO PLS_Event_Types VALUES ('Except', 'Exception occurred');
INSERT INTO PLS_Event_Types VALUES ('Metric', 'Metric logged');


CREATE TABLE PLS_Events (
  PLS_Event_Id INTEGER PRIMARY KEY,
  PLS_Event_Type_Cd VARCHAR2(10) NOT NULL REFERENCES PLS_Event_Types (PLS_Event_Type_Cd),
  TS TIMESTAMP NOT NULL
) PARTITION BY RANGE (TS) INTERVAL (INTERVAL '1' MONTH) (
  PARTITION P201508 VALUES LESS THAN (DATE '2015-08-01')
)
/

CREATE INDEX IE_PLS_Events_1 ON PLS_Events (TS)
/

CREATE TABLE PLS_Exec_Events (
  PLS_Event_Id INTEGER PRIMARY KEY,
  PLS_Unit_Obj_Owner VARCHAR2(30) NOT NULL,
  PLS_Unit_Concat_Nm VARCHAR2(255) NOT NULL,
  Edition_Nm VARCHAR2(30),
  Session_User VARCHAR2(30) NOT NULL,
  OS_User VARCHAR2(30) NOT NULL,
  Terminal VARCHAR2(30) NOT NULL,
  Module VARCHAR2(30) NOT NULL,
  Client_Id VARCHAR2(30),
  Calling_PLS_Exec_Event_Id INTEGER,
  CONSTRAINT FK_PLS_Exec_Events_1 FOREIGN KEY (PLS_Event_Id) REFERENCES PLS_Events (PLS_Event_Id)
) PARTITION BY REFERENCE (FK_PLS_Exec_Events_1)
/

CREATE INDEX IE_PLS_Exec_Events_1 ON PLS_Exec_Events (PLS_Unit_Obj_Owner, PLS_Unit_Concat_Nm) COMPRESS 2
/


CREATE TABLE PLS_Except_Events (
  PLS_Event_Id INTEGER PRIMARY KEY,
  PLS_Exec_Event_Id INTEGER NOT NULL,
  SQL_Cd NUMBER NOT NULL,
  SQL_Error_Msg VARCHAR2(512),
  Backtrace VARCHAR2(4000) NOT NULL,
  Call_Stack VARCHAR2(2000) NOT NULL,
  CONSTRAINT FK_PLS_Except_Events_1 FOREIGN KEY (PLS_Event_Id) REFERENCES PLS_Events (PLS_Event_Id),
  CONSTRAINT FK_PLS_Except_Events_2 FOREIGN KEY (PLS_Exec_Event_Id) REFERENCES PLS_Exec_Events (PLS_Event_Id)
) PARTITION BY REFERENCE (FK_PLS_Except_Events_1)
/


CREATE TABLE PLS_Metric_Events (
  PLS_Event_Id INTEGER PRIMARY KEY,
  PLS_Exec_Event_Id INTEGER NOT NULL,
  Step VARCHAR2(100),
  Val NUMBER,
  Completion_TS TIMESTAMP,
  CONSTRAINT FK_PLS_Metric_Events_1 FOREIGN KEY (PLS_Event_Id) REFERENCES PLS_Events (PLS_Event_Id),
  CONSTRAINT FK_PLS_Metric_Events_2 FOREIGN KEY (PLS_Exec_Event_Id) REFERENCES PLS_Exec_Events (PLS_Event_Id)
) PARTITION BY REFERENCE (FK_PLS_Metric_Events_1)
/


CREATE TABLE PLS_Event_Vars (
  PLS_Event_Id INTEGER NOT NULL,
  Var_Nm VARCHAR2(30),
  Var_Val ANYDATA,
  CONSTRAINT PK_PLS_Event_Vars PRIMARY KEY (PLS_Event_Id, Var_Nm),
  CONSTRAINT FK_PLS_Event_Vars_1 FOREIGN KEY (PLS_Event_Id) REFERENCES PLS_Events (PLS_Event_Id)
) PARTITION BY REFERENCE (FK_PLS_Event_Vars_1)
/


CREATE OR REPLACE VIEW V_PLS_Exec_Events AS
SELECT * FROM PLS_Events NATURAL JOIN PLS_Exec_Events
/

GRANT SELECT ON V_PLS_Exec_Events TO PUBLIC
/

CREATE OR REPLACE PUBLIC SYNONYM PLS_Exec_Events FOR V_PLS_Exec_Events
/


CREATE OR REPLACE VIEW V_PLS_Except_Events AS
SELECT
  E.PLS_Event_Id,
  E.TS, 
  Exc.SQL_Cd,
  Exc.SQL_Error_Msg,
  Exc.Backtrace,
  Exc.Call_Stack,
  Exe.PLS_Unit_Obj_Owner,
  Exe.PLS_Unit_Concat_Nm,
  Exe.Edition_Nm,
  Exe.Session_User,
  Exe.OS_User,
  Exe.Terminal,
  Exe.Module,
  Exe.Client_Id
FROM PLS_Events E
  JOIN PLS_Except_Events Exc ON Exc.PLS_Event_Id = E.PLS_Event_Id
  JOIN PLS_Exec_Events Exe ON Exe.PLS_Event_Id = Exc.PLS_Exec_Event_Id
/

GRANT SELECT ON V_PLS_Except_Events TO PUBLIC
/

CREATE OR REPLACE PUBLIC SYNONYM PLS_Except_Events FOR V_PLS_Except_Events
/


CREATE OR REPLACE VIEW V_PLS_Metric_Events AS
SELECT
  E.PLS_Event_Id,
  E.TS,
  M.Step,
  M.Val,
  M.Completion_TS - E.TS Duration,
  Exe.PLS_Unit_Obj_Owner,
  Exe.PLS_Unit_Concat_Nm,
  Exe.Edition_Nm,
  Exe.Session_User,
  Exe.OS_User,
  Exe.Terminal,
  Exe.Module,
  Exe.Client_Id
FROM PLS_Events E
  JOIN PLS_Metric_Events M ON M.PLS_Event_Id = E.PLS_Event_Id
  JOIN PLS_Exec_Events Exe ON Exe.PLS_Event_Id = M.PLS_Exec_Event_Id
/

GRANT SELECT ON V_PLS_Metric_Events TO PUBLIC
/

CREATE OR REPLACE PUBLIC SYNONYM PLS_Metric_Events FOR V_PLS_Metric_Events
/


CREATE OR REPLACE VIEW V_PLS_Event_Vars AS
SELECT
  E.PLS_Event_Id,
  E.PLS_Event_Type_Cd,
  E.TS,
  Exe.PLS_Unit_Obj_Owner,
  Exe.PLS_Unit_Concat_Nm,
  V.Var_Nm,
  V.Var_Val,
  ANYDATA.GETTYPENAME(V.Var_Val) Var_Type_Nm,
  ANYDATA.ACCESSVARCHAR2(V.Var_Val) Str_Var,
  ANYDATA.ACCESSNUMBER(V.Var_Val) Nbr_Var_Val,
  ANYDATA.ACCESSDATE(V.Var_Val) Date_Var_Val,
  ANYDATA.ACCESSTIMESTAMP(V.Var_Val) TS_Var_Val
FROM PLS_Events E
  JOIN PLS_Exec_Events Exe ON Exe.PLS_Event_Id = E.PLS_Event_Id
  JOIN PLS_Event_Vars V ON V.PLS_Event_Id = E.PLS_Event_Id
UNION ALL
SELECT
  E.PLS_Event_Id,
  E.PLS_Event_Type_Cd,
  E.TS,
  Exe.PLS_Unit_Obj_Owner,
  Exe.PLS_Unit_Concat_Nm,
  V.Var_Nm,
  V.Var_Val,
  ANYDATA.GETTYPENAME(V.Var_Val) Var_Type_Nm,
  ANYDATA.ACCESSVARCHAR2(V.Var_Val) Str_Var,
  ANYDATA.ACCESSNUMBER(V.Var_Val) Nbr_Var_Val,
  ANYDATA.ACCESSDATE(V.Var_Val) Date_Var_Val,
  ANYDATA.ACCESSTIMESTAMP(V.Var_Val) TS_Var_Val
FROM PLS_Events E
  JOIN PLS_Except_Events Exc ON Exc.PLS_Event_Id = E.PLS_Event_Id
  JOIN PLS_Exec_Events Exe ON Exe.PLS_Event_Id = Exc.PLS_Exec_Event_Id
  JOIN PLS_Event_Vars V ON V.PLS_Event_Id = E.PLS_Event_Id
UNION ALL
SELECT
  E.PLS_Event_Id,
  E.PLS_Event_Type_Cd,
  E.TS,
  Exe.PLS_Unit_Obj_Owner,
  Exe.PLS_Unit_Concat_Nm,
  V.Var_Nm,
  V.Var_Val,
  ANYDATA.GETTYPENAME(V.Var_Val) Var_Type_Nm,
  ANYDATA.ACCESSVARCHAR2(V.Var_Val) Str_Var,
  ANYDATA.ACCESSNUMBER(V.Var_Val) Nbr_Var_Val,
  ANYDATA.ACCESSDATE(V.Var_Val) Date_Var_Val,
  ANYDATA.ACCESSTIMESTAMP(V.Var_Val) TS_Var_Val
FROM PLS_Events E
  JOIN PLS_Metric_Events M ON M.PLS_Event_Id = E.PLS_Event_Id
  JOIN PLS_Exec_Events Exe ON Exe.PLS_Event_Id = M.PLS_Exec_Event_Id
  JOIN PLS_Event_Vars V ON V.PLS_Event_Id = E.PLS_Event_Id
/

GRANT SELECT ON V_PLS_Event_Vars TO PUBLIC
/

CREATE OR REPLACE PUBLIC SYNONYM PLS_Event_Vars FOR V_PLS_Event_Vars
/


CREATE TABLE A_PLS_Event_User_Access (
  Action_Cd CHAR(1) NOT NULL CHECK (Action_Cd IN ('I', 'U', 'D')),
  TS TIMESTAMP NOT NULL,
  Session_User VARCHAR2(30) NOT NULL,
  OS_User VARCHAR2(30) NOT NULL,
  Terminal VARCHAR2(30),
  Module VARCHAR2(48),
  Client_Id VARCHAR2(30),
  Owner VARCHAR2(30) NOT NULL,
  DB_User VARCHAR2(30) NOT NULL
)
/

CREATE OR REPLACE TRIGGER TC_A_PLS_Event_User_Access FOR INSERT ON A_PLS_Event_User_Access COMPOUND TRIGGER
    BEFORE EACH ROW IS
    BEGIN
      :NEW.TS := SYSTIMESTAMP;
      :NEW.Session_User := SYS_CONTEXT('USERENV', 'SESSION_USER');
      :NEW.OS_User := SYS_CONTEXT('USERENV', 'OS_USER');
      :NEW.Terminal := SYS_CONTEXT('USERENV', 'TERMINAL');
      :NEW.Module := SYS_CONTEXT('USERENV', 'MODULE');
      :NEW.Client_Id := SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER');
    END BEFORE EACH ROW;
END TC_A_PLS_Event_User_Access;
/


CREATE TABLE PLS_Event_User_Access (
  Owner VARCHAR2(30),
  DB_User VARCHAR2(30),
  CONSTRAINT PK_PLS_Event_User_Access PRIMARY KEY (Owner, DB_User)
) ORGANIZATION INDEX
/

CREATE OR REPLACE TRIGGER TC_PLS_Event_User_Access FOR INSERT OR UPDATE OR DELETE ON PLS_Event_User_Access COMPOUND TRIGGER
    AFTER EACH ROW IS
    BEGIN
        CASE
            WHEN INSERTING THEN
                INSERT INTO A_PLS_Event_User_Access (Action_Cd, Owner, DB_User)
                VALUES ('I', :NEW.Owner, :NEW.DB_User);
            WHEN UPDATING THEN
                INSERT INTO A_PLS_Event_User_Access (Action_Cd, Owner, DB_User)
                VALUES ('U', :NEW.Owner, :NEW.DB_User);
            WHEN DELETING THEN
                INSERT INTO A_PLS_Event_User_Access (Action_Cd, Owner, DB_User)
                VALUES ('D', :OLD.Owner, :OLD.DB_User);
        END CASE;
    END AFTER EACH ROW;
END TC_PLS_Event_User_Access;
/


CREATE OR REPLACE PACKAGE Pkg_PLS_Event_Access AS

    FUNCTION Allow_Access(i_User VARCHAR2, i_ObjName VARCHAR2) RETURN VARCHAR2;
    
    PROCEDURE Grant_Access(i_Owner VARCHAR2, i_DBUser VARCHAR2, i_Commit BOOLEAN := TRUE);
    
    PROCEDURE Remove_Access(i_Owner VARCHAR2, i_DBUser VARCHAR2, i_Commit BOOLEAN := TRUE);
    
END Pkg_PLS_Event_Access;
/


CREATE OR REPLACE PUBLIC SYNONYM PLS_Metric_Events FOR V_PLS_Metric_Events
/


CREATE OR REPLACE CONTEXT PLS_Logging_Context USING Pkg_PLS_Event_Mgr ACCESSED GLOBALLY
/

CREATE OR REPLACE PACKAGE Pkg_PLS_Event_Mgr AS


    SUBTYPE ST_LoggingLevel IS PLS_INTEGER RANGE 0 .. 9;
    
    gc_ExecEventTypeCd CONSTANT PLS_Event_Types.PLS_Event_Type_Cd%TYPE := 'Exec';
    
    gc_MetricEventTypeCd CONSTANT PLS_Event_Types.PLS_Event_Type_Cd%TYPE := 'Metric';
    
    gc_ExceptEventTypeCd CONSTANT PLS_Event_Types.PLS_Event_Type_Cd%TYPE := 'Except';
    
    
    FUNCTION Get_Logging_Level RETURN ST_LoggingLevel;
    
    PROCEDURE Set_Logging_Level(i_Val ST_LoggingLevel);
    
    
END Pkg_PLS_Event_Mgr;
/


CREATE OR REPLACE TYPE O_PLS_Unit AS OBJECT (

    Obj_Owner VARCHAR2(30),
    
    Concat_Nm VARCHAR2(255),
    
    CONSTRUCTOR FUNCTION O_PLS_Unit RETURN SELF AS RESULT
    
);
/


CREATE OR REPLACE TYPE BODY O_PLS_Unit IS


    CONSTRUCTOR FUNCTION O_PLS_Unit RETURN SELF AS RESULT IS
    
        l_Line NUMBER;
        
    BEGIN
    
        SELF.Obj_Owner := UTL_CALL_STACK.OWNER(3);
        
        SELF.Concat_Nm := UTL_CALL_STACK.CONCATENATE_SUBPROGRAM(UTL_CALL_STACK.SUBPROGRAM(3));
        
        RETURN;
        
    END O_PLS_Unit;
    
    
END;
/


GRANT EXECUTE ON O_PLS_Unit TO PUBLIC
/


CREATE OR REPLACE PUBLIC SYNONYM O_PLS_Unit FOR O_PLS_Unit
/


CREATE OR REPLACE TYPE O_PLS_Event_Var AS OBJECT (

    Nm VARCHAR2(30),
    
    Val ANYDATA,
    
    MEMBER PROCEDURE Log(i_PLSEventId INTEGER)
    
);
/


CREATE OR REPLACE TYPE BODY O_PLS_Event_Var AS


    MEMBER PROCEDURE Log(i_PLSEventId INTEGER) IS
    BEGIN
        INSERT INTO PLS_Event_Vars VALUES (i_PLSEventId, SELF.Nm, SELF.Val);
    END Log;
    
    
END;
/


CREATE OR REPLACE TYPE T_PLS_Event_Vars IS TABLE OF O_PLS_Event_Var
/


CREATE SEQUENCE S_PLS_Event_Id CACHE 1000
/


CREATE OR REPLACE TYPE O_PLS_Event AS OBJECT (

    Id INTEGER,
    
    Event_Type_Cd VARCHAR2(10),
    
    TS TIMESTAMP,
    
    Is_Logged INTEGER,
    
    Vars T_PLS_Event_Vars,
    
    
    MEMBER PROCEDURE Init(i_MinLoggingLevel INTEGER),
    
    MEMBER PROCEDURE Add_Var(i_Nm VARCHAR2, i_Val ANYDATA),
    
    NOT INSTANTIABLE MEMBER PROCEDURE Log,
    
    MEMBER PROCEDURE Log_Common
    
) NOT INSTANTIABLE NOT FINAL;
/


CREATE OR REPLACE TYPE BODY O_PLS_Event AS


    MEMBER PROCEDURE Init(i_MinLoggingLevel INTEGER) IS
    BEGIN
    
        SELF.Id := S_PLS_Event_Id.NEXTVAL;
        
        SELF.TS := SYSTIMESTAMP;
        
        SELF.Vars := T_PLS_Event_Vars();
        
        IF i_MinLoggingLevel <= Pkg_PLS_Event_Mgr.Get_Logging_Level THEN
            Log;
        ELSE
            SELF.Is_Logged := 0;
        END IF;
        
    END Init;
    
    
    MEMBER PROCEDURE Add_Var(i_Nm VARCHAR2, i_Val ANYDATA) IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        Vars.EXTEND;
        
        Vars(Vars.COUNT) := O_PLS_Event_Var(i_Nm, i_Val);
        
        IF Pkg_Data_Type_Utl.Get_Int_As_Boolean(SELF.Is_Logged) THEN
        
            Vars(Vars.COUNT).Log(SELF.Id);
            
            COMMIT;
            
        END IF;
        
    END Add_Var;
    
    
    MEMBER PROCEDURE Log_Common IS
    BEGIN
    
        INSERT INTO PLS_Events VALUES (SELF.Id, SELF.Event_Type_Cd, SELF.TS);
        
        FORALL i IN 1 .. SELF.Vars.COUNT
        INSERT INTO PLS_Event_Vars VALUES (SELF.Id, SELF.Vars(i).Nm, SELF.Vars(i).Val);
        
        SELF.Is_Logged := 1;
        
    END Log_Common;
    
    
END;
/


CREATE OR REPLACE TYPE O_PLS_Exec_Event UNDER O_PLS_Event (

    PLS_Unit O_PLS_Unit,
    
    Calling_Exec_Event_Id INTEGER,
    
    CONSTRUCTOR FUNCTION O_PLS_Exec_Event(
      i_CallingExecEventId INTEGER := NULL,
      i_MinLoggingLevel INTEGER := 3
    ) RETURN SELF AS RESULT,
    
    OVERRIDING MEMBER PROCEDURE Log
        
);
/


CREATE OR REPLACE TYPE BODY O_PLS_Exec_Event AS


    CONSTRUCTOR FUNCTION O_PLS_Exec_Event(
      i_CallingExecEventId INTEGER := NULL,
      i_MinLoggingLevel INTEGER := 3
    ) RETURN SELF AS RESULT IS
    BEGIN
    
        SELF.Event_Type_Cd := Pkg_PLS_Event_Mgr.gc_ExecEventTypeCd;
        
        SELF.PLS_Unit := O_PLS_Unit();
        
        SELF.Calling_Exec_Event_Id := i_CallingExecEventId;
        
        SELF.Init(i_MinLoggingLevel);
        
        RETURN;
        
    END O_PLS_Exec_Event;
    
    
    OVERRIDING MEMBER PROCEDURE Log IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        SELF.Log_Common;
        
        INSERT INTO PLS_Exec_Events (
          PLS_Event_Id,
          PLS_Unit_Obj_Owner,
          PLS_Unit_Concat_Nm,
          Edition_Nm,
          Session_User,
          OS_User,
          Terminal,
          Module,
          Client_Id,
          Calling_PLS_Exec_Event_Id
        ) VALUES (
          SELF.Id,
          SELF.PLS_Unit.Obj_Owner,
          SELF.PLS_Unit.Concat_Nm,
          SYS_CONTEXT('USERENV', 'CURRENT_EDITION_NAME'),
          SYS_CONTEXT('USERENV', 'SESSION_USER'),
          SYS_CONTEXT('USERENV', 'OS_USER'),
          SYS_CONTEXT('USERENV', 'TERMINAL'),
          SYS_CONTEXT('USERENV', 'MODULE'),
          SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER'),
          SELF.Calling_Exec_Event_Id
        );
        
        COMMIT;
        
    END Log;
    
    
END;
/


GRANT EXECUTE ON O_PLS_Exec_Event TO PUBLIC
/


CREATE PUBLIC SYNONYM O_PLS_Exec_Event FOR O_PLS_Exec_Event
/

CREATE OR REPLACE TYPE O_PLS_Except_Event UNDER O_PLS_Event (

    
    Exec_Event O_PLS_Exec_Event,
    
    SQL_Cd NUMBER,
    
    SQL_Error_Msg VARCHAR2(512),
    
    Backtrace VARCHAR2(4000),
    
    Call_Stack VARCHAR2(2000),
    
    
    CONSTRUCTOR FUNCTION O_PLS_Except_Event(
      i_ExecEvent O_PLS_Exec_Event,
      i_Backtrace VARCHAR2 := NULL,
      i_CallStack VARCHAR2 := NULL,
      i_MinLoggingLevel INTEGER := 3
    ) RETURN SELF AS RESULT,
    
    OVERRIDING MEMBER PROCEDURE Log

        
);
/


CREATE OR REPLACE TYPE BODY O_PLS_Except_Event AS


    CONSTRUCTOR FUNCTION O_PLS_Except_Event(
      i_ExecEvent O_PLS_Exec_Event,
      i_Backtrace VARCHAR2 := NULL,
      i_CallStack VARCHAR2 := NULL,
      i_MinLoggingLevel INTEGER := 3
    ) RETURN SELF AS RESULT IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        SELF.Event_Type_Cd := Pkg_PLS_Event_Mgr.gc_ExceptEventTypeCd;
        
        SELF.Exec_Event := i_ExecEvent;
        
        SELF.SQL_Cd := SQLCODE;
        
        SELF.SQL_Error_Msg := SQLERRM;
        
        SELF.Backtrace := NVL(i_Backtrace, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        
        SELF.Call_Stack := NVL(i_CallStack, DBMS_UTILITY.FORMAT_CALL_STACK);
        
        SELF.Init(i_MinLoggingLevel);
        
        RETURN;
        
    END O_PLS_Except_Event;
    
    
    OVERRIDING MEMBER PROCEDURE Log IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        IF NOT Pkg_Data_Type_Utl.Get_Int_As_Boolean(SELF.Exec_Event.Is_Logged) THEN
            SELF.Exec_Event.Log;
        END IF;
        
        SELF.Log_Common;
        
        INSERT INTO PLS_Except_Events
        VALUES (SELF.Id, SELF.Exec_Event.Id, SELF.SQL_Cd, SELF.SQL_Error_Msg, SELF.Backtrace, SELF.Call_Stack);
        
        COMMIT;
        
    END Log;
    
    
END;
/


GRANT EXECUTE ON O_PLS_Except_Event TO PUBLIC
/


CREATE PUBLIC SYNONYM O_PLS_Except_Event FOR O_PLS_Except_Event
/


CREATE OR REPLACE TYPE O_PLS_Metric_Event UNDER O_PLS_Event (
    
    Exec_Event O_PLS_Exec_Event,
    
    Step VARCHAR2(100),
    
    Val NUMBER,
    
    Completion_TS TIMESTAMP(9),
    
    CONSTRUCTOR FUNCTION O_PLS_Metric_Event(
      i_ExecEvent O_PLS_Exec_Event,
      i_Step VARCHAR2,
      i_MinLoggingLevel INTEGER := 7
    ) RETURN SELF AS RESULT,
    
    MEMBER PROCEDURE Complete(i_Val NUMBER),
    
    OVERRIDING MEMBER PROCEDURE Log

);
/


CREATE OR REPLACE TYPE BODY O_PLS_Metric_Event AS


    CONSTRUCTOR FUNCTION O_PLS_Metric_Event(
      i_ExecEvent O_PLS_Exec_Event,
      i_Step VARCHAR2,
      i_MinLoggingLevel INTEGER := 7
    ) RETURN SELF AS RESULT IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        SELF.Event_Type_Cd := Pkg_PLS_Event_Mgr.gc_MetricEventTypeCd;
        
        SELF.Exec_Event := i_ExecEvent;
        
        SELF.Step := i_Step;
        
        SELF.Init(i_MinLoggingLevel);
        
        RETURN;
        
    END O_PLS_Metric_Event;
    
    
    MEMBER PROCEDURE Complete(i_Val NUMBER) IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        SELF.Val := i_Val;
        
        SELF.Completion_TS := SYSTIMESTAMP;
        
        IF Pkg_Data_Type_Utl.Get_Int_As_Boolean(SELF.Is_Logged) THEN
        
            UPDATE PLS_Metric_Events SET Val = SELF.Val, Completion_TS = SELF.Completion_TS WHERE PLS_Event_Id = SELF.Id;
            
        END IF;
        
        COMMIT;
        
    END Complete;
    
    
    OVERRIDING MEMBER PROCEDURE Log IS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
    
        IF NOT Pkg_Data_Type_Utl.Get_Int_As_Boolean(SELF.Exec_Event.Is_Logged) THEN
            SELF.Exec_Event.Log;
        END IF;
        
        SELF.Log_Common;
        
        INSERT INTO PLS_Metric_Events (PLS_Event_Id, PLS_Exec_Event_Id, Step) VALUES (SELF.Id, SELF.Exec_Event.Id, SELF.Step);
        
        COMMIT;
        
    END Log;

    
END;
/


GRANT EXECUTE ON O_PLS_Metric_Event TO PUBLIC
/


CREATE PUBLIC SYNONYM O_PLS_Metric_Event FOR O_PLS_Metric_Event
/


CREATE OR REPLACE PACKAGE BODY Pkg_PLS_Event_Mgr AS


    FUNCTION Get_Logging_Level RETURN ST_LoggingLevel IS
    BEGIN
        RETURN SYS_CONTEXT('PLS_Logging_Context', 'Level');
    END Get_Logging_Level;
    
    
    PROCEDURE Set_Logging_Level(i_Val ST_LoggingLevel) IS
    BEGIN
        DBMS_SESSION.SET_CONTEXT('PLS_Logging_Context', 'Level', i_Val);
    END Set_Logging_Level;
    
    
BEGIN

    IF SYS_CONTEXT('PLS_Logging_Context', 'Level') IS NULL THEN
        Set_Logging_Level(5);
    END IF;
    
END Pkg_PLS_Event_Mgr;
/


CREATE OR REPLACE PACKAGE BODY Pkg_PLS_Event_Access IS


    FUNCTION Allow_Access(i_User VARCHAR2, i_ObjName VARCHAR2) RETURN VARCHAR2 IS
    
        l_Return VARCHAR2(200);
        
    BEGIN
    
        l_Return := 'PLS_Unit_Obj_Owner = USER ' ||
                 'OR EXISTS (SELECT 1 ' ||
                            'FROM PLS_Event_User_Access ' ||
                            'WHERE Owner = PLS_Unit_Obj_Owner AND DB_User = USER)';
                            
        RETURN l_Return;
        
    END Allow_Access;
    
    
    PROCEDURE Grant_Access(i_Owner VARCHAR2, i_DBUser VARCHAR2, i_Commit BOOLEAN) IS
    
        l_PLSExecEvent O_PLS_Exec_Event;
        
        l_PLSExceptEvent O_PLS_Except_Event;
        
        l_InvalidSQLCd EXCEPTION;
        
        PRAGMA EXCEPTION_INIT(l_InvalidSQLCd, -2290);
        
        PROCEDURE Log_PLS_Exec IS
        BEGIN
        
            l_PLSExecEvent := O_PLS_Exec_Event(i_MinLoggingLevel => 7);
            
            l_PLSExecEvent.Add_Var('i_Owner', ANYDATA.CONVERTVARCHAR2(i_Owner));
            
            l_PLSExecEvent.Add_Var('i_DBUser', ANYDATA.CONVERTVARCHAR2(i_DBUser));
            
        END Log_PLS_Exec;
        
    BEGIN
    
        Log_PLS_Exec;
            
        INSERT INTO PLS_Event_User_Access VALUES (UPPER(i_Owner), UPPER(i_DBUser));
        
        IF i_Commit THEN
            COMMIT;
        END IF;
        
    EXCEPTION
    
        WHEN OTHERS THEN
        
            l_PLSExceptEvent := O_PLS_Except_Event(
              l_PLSExecEvent,
              DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
              DBMS_UTILITY.FORMAT_CALL_STACK
            );
            
            RAISE;
            
    END Grant_Access;
    
    
    PROCEDURE Remove_Access(i_Owner VARCHAR2, i_DBUser VARCHAR2, i_Commit BOOLEAN := TRUE) IS
    
        l_PLSExecEvent O_PLS_Exec_Event;
        
        l_PLSExceptEvent O_PLS_Except_Event;
        
        l_InvalidSQLCd EXCEPTION;
        
        PRAGMA EXCEPTION_INIT(l_InvalidSQLCd, -2290);
        
        PROCEDURE Log_PLS_Exec IS
        BEGIN
        
            l_PLSExecEvent := O_PLS_Exec_Event(i_MinLoggingLevel => 7);
            
            l_PLSExecEvent.Add_Var('i_Owner', ANYDATA.CONVERTVARCHAR2(i_Owner));
            
            l_PLSExecEvent.Add_Var('i_DBUser', ANYDATA.CONVERTVARCHAR2(i_DBUser));
            
        END Log_PLS_Exec;
        
    BEGIN
    
        Log_PLS_Exec;
            
        DELETE PLS_Event_User_Access WHERE Owner = UPPER(i_Owner) AND DB_User = UPPER(i_DBUser);
        
        IF i_Commit THEN
            COMMIT;
        END IF;
        
    EXCEPTION
    
        WHEN OTHERS THEN
        
            l_PLSExceptEvent := O_PLS_Except_Event(
              l_PLSExecEvent,
              DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
              DBMS_UTILITY.FORMAT_CALL_STACK
            );
            
            RAISE;
            
    END Remove_Access;
    
    
END Pkg_PLS_Event_Access;
/


BEGIN
  DBMS_RLS.Add_Policy(
    object_schema => 'PLS_UTL',
    object_name => 'PLS_EXEC_EVENTS',
    policy_name => 'Exec Event Access',
    policy_function => 'Pkg_PLS_Event_Access.Allow_Access'
  );
END;
/
