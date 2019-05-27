create or replace package STG_GENERAL_P is

  -- Author  : 03517365
  -- Created : 04/02/2005 13:57:33
  -- Purpose : Funciones y procedimientos generales de la STG
  
  -- Public type declarations
  --type <TypeName> is <Datatype>;
  
  -- Public constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Public variable declarations
  --<VariableName> <Datatype>;

  -- Public function and procedure declarations
PROCEDURE INICIA_LOG;
FUNCTION INICIA_LOG_PASO(V_PROCEDIMIENTO VARCHAR2 default '-', V_PARAMETROS VARCHAR2 DEFAULT '-') return number;
PROCEDURE FIN_LOG_PASO(V_ID_PASO number, V_RESULTADO VARCHAR2 default 'Fin', V_RESULTADO_OK NUMBER default 0, V_RESULTADO_KO NUMBER default 0, V_WARNINGS NUMBER default 0);  
end STG_GENERAL_P;
/
create or replace package body STG_GENERAL_P is

  -- Private type declarations
  --type <TypeName> is <Datatype>;
  
  -- Private constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Private variable declarations
  --<VariableName> <Datatype>;

  -- Function and procedure implementations
   

  -- Initialization

PROCEDURE INICIA_LOG as 
V_ID_LOG number;
begin
	select stg_id_log_s.nextval into v_id_log from dual;
end;


FUNCTION INICIA_LOG_PASO(V_PROCEDIMIENTO VARCHAR2 default '-', V_PARAMETROS VARCHAR2 DEFAULT '-') return number is


V_ID_LOG number;
v_id_paso number;

BEGIN
	select stg_id_log_s.currval into v_id_log from dual;
	select count(*) into v_id_paso from stg_log where id_log=V_ID_LOG;

	insert into stg_log(id_log,id_paso, fecha_inicio, procedimiento, parametros)
	values (v_id_log,v_id_paso, sysdate, v_procedimiento, v_parametros);

	COMMIT;
    return v_id_paso;

END;


PROCEDURE FIN_LOG_PASO(V_ID_PASO number, 
                       V_RESULTADO VARCHAR2 default 'Fin', 
                       V_RESULTADO_OK NUMBER default 0, 
                       V_RESULTADO_KO NUMBER default 0,
                       V_WARNINGS NUMBER default 0) is
V_ID_LOG number;
  

BEGIN

	select stg_id_log_s.currval into v_id_log from dual;

	update stg_log set 
		fecha_fin = sysdate,
       	duracion = 24*3600*(sysdate-fecha_inicio),
		resultado = v_resultado,
		resultado_ok = v_resultado_ok,
		resultado_ko = v_resultado_ko,
    warnings = v_warnings
	where 
		ID_LOG= v_id_log and
		ID_PASO=V_ID_PASO;

	COMMIT;
END;

BEGIN
     null;  
END STG_GENERAL_P;
/
