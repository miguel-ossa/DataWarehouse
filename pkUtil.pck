create or replace package pkUtil is

  -- Author  : MOSSA
  -- Created : 30/12/2004 7:31:09
  -- Purpose : Utilidades varias
  
  -- Public type declarations
  --type <TypeName> is <Datatype>;
  
  -- Public constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Public variable declarations
  --<VariableName> <Datatype>;

  -- Public function and procedure declarations
  --function <FunctionName>(<Parameter> <Datatype>) return <Datatype>;
  procedure BuscaCol(p_Columna varchar2
                    ,p_Tabla varchar2 default null);

end pkUtil;
/
create or replace package body pkUtil is

  -- Private type declarations
  --type <TypeName> is <Datatype>;
  
  -- Private constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Private variable declarations
  --<VariableName> <Datatype>;

  -- Function and procedure implementations
  procedure BuscaCol(p_Columna varchar2, p_Tabla varchar2 default null) is
    cursor c1 is
      select table_name
            ,column_name
            ,data_type
            ,data_length
        from all_tab_columns
        where column_name like upper('%' || p_Columna || '%') and
              table_name like upper('%' || p_Tabla || '%');
    cursor c2 is
      select table_name
            ,column_name
            ,data_type
            ,data_length
        from all_tab_columns
        where column_name like upper('%' || p_Columna || '%');
  begin
    if ( p_Tabla is null ) then
      for c in c2 loop
        dbms_output.put_line(c.table_name || ' - ' ||
                            c.column_name || ' - ' ||
                            c.data_type || ' - ' ||
                            c.data_length);
      end loop;
    else
      for c in c1 loop
        dbms_output.put_line(c.table_name || ' - ' ||
                            c.column_name || ' - ' ||
                            c.data_type || ' - ' ||
                            c.data_length);
      end loop;
    end if;
  end BuscaCol;

begin
  -- Initialization
  null;
end pkUtil;
/
