create or replace package pkCarga_ETL is

  -- Author  : MOSSA
  -- Created : 15/12/2004 14:05:29
  -- Purpose : Cargar las tablas DataWarehouse
  
  -- Public type declarations
  --type <TypeName> is <Datatype>;
  
  -- Public constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Public variable declarations
  --<VariableName> <Datatype>;

  --Modelo estrella de ventas:  
  procedure Cargar_Agr_Ventas1;
  procedure Cargar_Fact_Ventas;
  procedure Cargar_Dim_Meses;
  procedure Cargar_Dim_Paises;
  procedure Cargar_Dim_Provincias;
  procedure Cargar_Dim_Tiendas;
  procedure Cargar_Dim_Familias;
  procedure Cargar_Dim_Tejidos;
  procedure Cargar_Dim_Modelos;
  --Llama a todas las cargas de arriba
  procedure Cargar_Destino_Ventas;
  --
  procedure Comprueba_Zonas(p_id number);
  procedure Comprueba_Tejidos(p_id number);
  procedure Des_Fk_Destino;
  procedure Act_Fk_Destino;
  --Modelo estrella de precios:
  procedure Cargar_Fact_Precios;
  procedure Cargar_Dim_Tarifas;
  procedure Cargar_Destino_Precios;
  procedure Borrar_Dimensiones;
end pkCarga_ETL;
/
create or replace package body pkCarga_ETL is
  -- Private type declarations
  --type <TypeName> is <Datatype>;
  
  -- Private constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Private variable declarations
  vComando varchar2(500);              

  -- Function and procedure implementations

  /*---------------*\
  | Rutinas de log. |
  \*---------------*/  
  function inicio_log(p_procedimiento varchar2
                      ,p_paso varchar2 default null) return number as
    nuevo_id number;
  begin
    select nvl(max(id + 1), 1) 
        into nuevo_id 
        from destino.etl_log;
    insert into destino.etl_log
        (id, procedimiento, paso, fecha_inicio)
      values 
        (nuevo_id, p_procedimiento, p_paso, sysdate);
    return nuevo_id;
    commit;
  end;
  
  procedure fin_log(p_id number) as
  begin
    update destino.etl_log 
      set fecha_fin = sysdate,
          duracion = (sysdate - fecha_inicio) * 24 * 3600
      where id = p_id;
    commit;
  end;
  
  procedure error_log(p_texto_err varchar2
                      ,p_id number) is
  begin
    insert into destino.etl_log_err
        (id, texto_err)
      values
        (p_id, p_texto_err);
    commit;
  end;
  
  /*----------------------------------------------------------------------*\
  | A estas rutinas debe pasársele el id (log) del procedimiento llamador. |
  \*----------------------------------------------------------------------*/
  procedure Comprueba_Zonas(p_id number) is
    cursor c_zonas is
        select t.id_tienda, z.id_zona_influencia
          from origen.tiendas t
            inner join origen.tienda_zona z 
                on z.id_tienda = t.id_tienda
          where t.id_tienda in 
            (select id_tienda 
                from origen.tienda_zona 
                group by id_tienda 
                having count(*) > 1);
    cursor c_descr_zonas is
        select t.id_tienda
              ,z.id_zona_influencia
              ,z.zona_influencia 
          from origen.tiendas t
            left join origen.tienda_zona tz 
                on tz.id_tienda = t.id_tienda
            left join origen.zonas_influencia z 
                on z.id_zona_influencia = tz.id_zona_influencia
          where length(rtrim(z.zona_influencia, ' ')) > 25;
  begin
    for c in c_zonas loop 
      error_log('Tienda ' || 
                c.id_tienda || 
                ' asignada a zona ' || 
                c.id_zona_influencia
                ,p_id);
    end loop;
    for c in c_descr_zonas loop
      error_log('Tienda ' ||
                c.id_tienda ||
                ' con zona ' ||
                c.id_zona_influencia ||
                ' mayor de 25 caracteres : ' ||
                c.zona_influencia
                ,p_id);
    end loop;
  end Comprueba_Zonas;
  
  procedure Comprueba_Tejidos(p_id number) is
  begin
    for c in (
      select id_modelo, m.id_tejido from origen.modelos m
          left join origen.tejidos t on t.id_tejido = m.id_tejido
          where t.id_tejido is null        
              ) loop
        error_log('Modelo ' ||
                  c.id_modelo ||
                ' con id_tejido inexistente : ' ||
                c.id_tejido
                ,p_id);
    end loop;
  end Comprueba_Tejidos;
  
  /*---------------------------------------------------------------------------*\
   | Modelo estrella de ventas                                                 |
  \*---------------------------------------------------------------------------*/
  procedure Cargar_Dim_Meses is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Meses');
    --Modificaciones
    UPDATE
      (SELECT
            dmes.anyo           old_anyo
            ,dmes.num_mes       old_num_mes
            ,dmes.mes           old_mes
            ,fecha_modificacion fecha_modificacion
            --
            ,omes.anyo          new_anyo
            ,omes.num_mes       new_num_mes
            ,omes.mes           new_mes
          FROM
            origen.meses omes
            LEFT JOIN destino.dim_meses dmes ON dmes.id_mes = omes.id_mes
          WHERE dmes.id_mes IS NOT NULL AND
                (dmes.anyo <> omes.anyo OR
                dmes.mes <> omes.mes OR
                dmes.num_mes <> omes.num_mes)
      )
      SET old_anyo    = new_anyo
          ,old_num_mes = new_num_mes
          ,old_mes     = new_mes
          ,fecha_modificacion = SYSDATE;
    --Altas      
    INSERT INTO destino.dim_meses
      (id_mes
      ,anyo
      ,num_mes
      ,mes
      ,fecha_alta)
      (SELECT
            omes.id_mes
            ,omes.anyo
            ,omes.num_mes
            ,omes.mes
            ,SYSDATE
        FROM 
            origen.meses omes
            LEFT JOIN destino.dim_meses dmes ON dmes.id_mes = omes.id_mes
        WHERE dmes.id_mes IS NULL);
    --Bajas
    UPDATE
      (SELECT
            dmes.fecha_baja fecha_baja
        FROM
            destino.dim_meses dmes
            LEFT JOIN origen.meses omes ON omes.id_mes = dmes.id_mes
        WHERE omes.id_mes IS NULL
      )
      SET fecha_baja = SYSDATE;     
    COMMIT;
    fin_log(id);
  end Cargar_Dim_Meses;

  procedure Cargar_Dim_Paises is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Paises');
    --Modificaciones
    UPDATE
      (SELECT
            dpai.desc_pais      old_desc_pais
            ,fecha_modificacion fecha_modificacion
            ,opai.desc_pais      new_desc_pais
        FROM
            origen.paises opai
            LEFT JOIN destino.dim_paises dpai ON dpai.id_pais = opai.id_pais
        WHERE dpai.id_pais IS NOT NULL AND
              dpai.desc_pais <> opai.desc_pais)
      SET old_desc_pais = new_desc_pais,
          fecha_modificacion = SYSDATE;
    --Altas    
    INSERT INTO destino.dim_paises
      (id_pais
      ,desc_pais
      ,fecha_alta)
      (SELECT
            opai.id_pais
            ,opai.desc_pais
            ,SYSDATE
        FROM 
            origen.paises opai
            LEFT JOIN destino.dim_paises dpai ON dpai.id_pais = opai.id_pais
        WHERE dpai.id_pais IS NULL); 
    --Bajas
    UPDATE
      (SELECT 
            dpai.fecha_baja fecha_baja
        FROM
            destino.dim_paises dpai
            LEFT JOIN origen.paises opai ON opai.id_pais = dpai.id_pais
        WHERE opai.id_pais IS NULL
      )
      SET fecha_baja = SYSDATE;    
    COMMIT;
    fin_log(id);
  end Cargar_Dim_Paises;

  procedure Cargar_Dim_Provincias is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Provincias');
    --Modificaciones
    UPDATE 
      (SELECT 
            dprov.desc_provincia old_desc_provincia
            ,dprov.id_pais        old_id_pais
            ,fecha_modificacion   fecha_modificacion
            --
            ,oprov.desc_provincia new_desc_provincia
            ,oprov.id_pais        new_id_pais
        FROM
            origen.provincias oprov
            LEFT JOIN destino.dim_provincias dprov 
                ON dprov.id_provincia = oprov.id_provincia
        WHERE dprov.id_provincia IS NOT NULL AND
              (dprov.desc_provincia <> oprov.desc_provincia OR
              dprov.id_pais <> oprov.id_pais)
      )
      SET old_desc_provincia    = new_desc_provincia
          ,old_id_pais          = new_id_pais
          ,fecha_modificacion   = SYSDATE;
    --Altas           
    INSERT INTO destino.dim_provincias 
      (id_pais
      ,id_provincia
      ,desc_provincia
      ,fecha_alta)
      (SELECT oprov.id_pais, oprov.id_provincia, oprov.desc_provincia, SYSDATE
        FROM
            origen.provincias oprov
            LEFT JOIN destino.dim_provincias dprov 
                ON dprov.id_provincia = oprov.id_provincia
        WHERE dprov.id_provincia IS NULL);
    --Bajas
    UPDATE
      (SELECT
            dprov.fecha_baja fecha_baja
        FROM
            destino.dim_provincias dprov
            LEFT JOIN origen.provincias oprov ON oprov.id_provincia = dprov.id_provincia
        WHERE oprov.id_provincia IS NULL
      )
      SET fecha_baja = SYSDATE;
    COMMIT;
    fin_log(id);
  end Cargar_Dim_Provincias;

  procedure Cargar_Dim_Tiendas is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Tiendas');
    MERGE INTO destino.dim_tiendas des_tie
      USING (
        SELECT COUNT(*)           contador
              ,tiendas.id_tienda 
              ,tienda
              ,tipo_tienda
              ,id_provincia
              ,vendedor_003
              ,MAX(zona_influencia) zona_influencia
          FROM 
              origen.tiendas
              LEFT JOIN origen.tienda_zona 
                  ON (tiendas.id_tienda=tienda_zona.id_tienda)
              LEFT JOIN origen.zonas_influencia 
                  ON (zonas_influencia.id_zona_influencia=tienda_zona.id_zona_influencia)
          GROUP BY
              tiendas.id_tienda
              ,tienda
              ,tipo_tienda
              ,id_provincia
              ,vendedor_003
      ) ori_tie
      ON (des_tie.id_tienda = ori_tie.id_tienda)
    WHEN MATCHED THEN
      UPDATE SET zona_influencia=decode(contador,1,substr(ori_tie.zona_influencia,1,25),NULL)
                ,tienda = ori_tie.tienda
                ,tipo_tienda = ori_tie.tipo_tienda
                ,id_provincia = ori_tie.id_provincia
                ,vendedor_003 = ori_tie.vendedor_003
                ,fecha_modificacion = SYSDATE
    WHEN NOT MATCHED THEN
      INSERT (id_tienda 
              ,tienda
              ,tipo_tienda
              ,id_provincia
              ,vendedor_003
              ,zona_influencia
              ,fecha_alta)
        VALUES 
              (ori_tie.id_tienda
              ,ori_tie.tienda
              ,ori_tie.tipo_tienda
              ,ori_tie.id_provincia
              ,ori_tie.vendedor_003
              ,decode(contador,1,substr(ori_tie.zona_influencia, 1, 25),NULL)
              ,SYSDATE);
    --Bajas
    UPDATE
      (SELECT
            dtie.fecha_baja fecha_baja
        FROM
            destino.dim_tiendas dtie
            LEFT JOIN origen.tiendas otie ON otie.id_tienda = dtie.id_tienda
        WHERE otie.id_tienda IS NULL
      )
      SET fecha_baja = SYSDATE;
    COMMIT;          
    fin_log(id);
  end Cargar_Dim_Tiendas;

  procedure Cargar_Dim_Familias is
    id number;
  begin
    id := inicio_log('Cargar_Destino_Precios');
    --Actualizaciones
    UPDATE 
      (SELECT 
            dfam.familia              old_familia
            ,dfam.fecha_modificacion  fecha_modificacion
            ,ofam.familia             new_familia
        FROM
            origen.familias ofam
            LEFT JOIN destino.dim_familias dfam ON dfam.id_familia = ofam.id_familia
        WHERE dfam.id_familia IS NOT NULL AND
              dfam.familia <> ofam.familia)
      SET old_familia = new_familia,
          fecha_modificacion = SYSDATE;
    --Altas           
    INSERT INTO destino.dim_familias
      (id_familia
      ,familia
      ,fecha_alta)
      (SELECT 
            ofam.id_familia, ofam.familia, SYSDATE
        FROM
            origen.familias ofam
            LEFT JOIN destino.dim_familias dfam ON dfam.id_familia = ofam.id_familia
        WHERE dfam.id_familia IS NULL);
    --Bajas
    UPDATE
      (SELECT
            dfam.fecha_baja fecha_baja
        FROM
            destino.dim_familias dfam
            LEFT JOIN origen.familias ofam ON ofam.id_familia = dfam.id_familia
        WHERE ofam.id_familia IS NULL
      )
      SET fecha_baja = SYSDATE;
    COMMIT;
    fin_log(id);
  end Cargar_Dim_Familias;

  procedure Cargar_Dim_Tejidos is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Tejidos');
    --Actualizaciones
    UPDATE 
      (SELECT 
            dtej.tejido               old_tejido
            ,dtej.fecha_modificacion  fecha_modificacion
            ,otej.tejido              new_tejido
        FROM
            origen.tejidos otej
            LEFT JOIN destino.dim_tejidos dtej ON dtej.id_tejido = otej.id_tejido
        WHERE dtej.id_tejido IS NOT NULL AND
              dtej.tejido <> otej.tejido)
      SET old_tejido = new_tejido
          ,fecha_modificacion = SYSDATE;
    --Altas           
    INSERT INTO destino.dim_tejidos
      (id_tejido
      ,tejido
      ,fecha_alta)
      (SELECT 
            otej.id_tejido
            ,otej.tejido
            ,SYSDATE
        FROM
            origen.tejidos otej
            LEFT JOIN destino.dim_tejidos dtej ON dtej.id_tejido = otej.id_tejido
        WHERE dtej.id_tejido IS NULL);
    --Bajas
    UPDATE
      (SELECT 
            dtej.fecha_baja fecha_baja
        FROM
            destino.dim_tejidos dtej
            LEFT JOIN origen.tejidos otej ON otej.id_tejido = dtej.id_tejido
        WHERE otej.id_tejido IS NULL
      )
      SET fecha_baja = SYSDATE;
    COMMIT;
    fin_log(id);
  end Cargar_Dim_Tejidos;

  procedure Cargar_Dim_Modelos is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Modelos');
    --Modificaciones
    UPDATE 
      (SELECT 
            dmod.modelo               old_modelo
            ,dmod.temporada           old_temporada
            ,dmod.id_familia          old_id_familia
            ,dmod.id_tejido           old_id_tejido
            ,dmod.fecha_modificacion  fecha_modificacion
            --
            ,omod.modelo              new_modelo
            ,omod.temporada           new_temporada
            ,omod.id_familia          new_id_familia
            ,omod.id_tejido           new_id_tejido
        FROM
            origen.modelos omod
            LEFT JOIN destino.dim_modelos dmod ON dmod.id_modelo = omod.id_modelo
        WHERE dmod.id_modelo IS NOT NULL AND
              (dmod.modelo <> omod.modelo OR
              dmod.temporada <> omod.temporada OR
              dmod.id_familia <> omod.id_familia OR
              dmod.id_tejido <> omod.id_tejido)
      )
      SET old_modelo = new_modelo
          ,old_temporada = new_temporada
          ,old_id_familia = new_id_familia
          ,old_id_tejido = new_id_tejido
          ,fecha_modificacion = SYSDATE;
    --Altas      
    INSERT INTO destino.dim_modelos 
      (id_modelo
      ,modelo
      ,id_familia
      ,id_tejido
      ,fecha_alta)
      (SELECT 
            omod.id_modelo
            ,omod.modelo
            ,omod.id_familia
            ,omod.id_tejido
            ,SYSDATE
        FROM
            origen.modelos omod
            LEFT JOIN destino.dim_modelos dmod ON dmod.id_modelo = omod.id_modelo
        WHERE dmod.id_modelo IS NULL);
    --Bajas       
    UPDATE
      (SELECT
            dmod.fecha_baja fecha_baja
        FROM 
            destino.dim_modelos dmod
            LEFT JOIN origen.modelos omod ON omod.id_modelo = dmod.id_modelo
        WHERE omod.id_modelo IS NULL   
      )
      SET fecha_baja = SYSDATE;
    COMMIT;
    fin_log(id);
  end Cargar_Dim_Modelos;

  procedure Cargar_Fact_Ventas is
    id number;
  begin
    id := inicio_log('Cargar_Fact_Ventas');
    MERGE INTO destino.fact_ventas dvta
      USING(
        SELECT 
              omes.id_mes                           id_mes
              ,opai.id_pais                         id_pais
              ,opro.id_provincia                    id_provincia
              ,otie.id_tienda                       id_tienda
              ,ofam.id_familia                      id_familia  
              ,NVL(otej.id_tejido,'0')              id_tejido
              ,omod.id_modelo                       id_modelo
              ,SUM(oven.unidades)                   unidades
              ,SUM(oven.unidades * oven.pvp_unidad) pvp_total
          FROM 
              origen.ventas oven   
              INNER JOIN origen.fechas ofec ON oven.id_fecha = ofec.id_fecha
              INNER JOIN origen.meses omes ON ofec.id_mes = omes.id_mes
              INNER JOIN origen.modelos omod ON oven.id_modelo = omod.id_modelo
              INNER JOIN origen.familias ofam ON omod.id_familia = ofam.id_familia
              LEFT JOIN origen.tejidos otej ON omod.id_tejido = otej.id_tejido
              INNER JOIN origen.tiendas otie ON oven.id_tienda = otie.id_tienda
              INNER JOIN origen.provincias opro ON otie.id_provincia = opro.id_provincia
              INNER JOIN origen.paises opai ON opro.id_pais = opai.id_pais
          GROUP BY 
              omes.id_mes
              ,opai.id_pais
              ,opro.id_provincia            
              ,otie.id_tienda
              ,ofam.id_familia
              ,NVL(otej.id_tejido,'0')
              ,omod.id_modelo
        ) ovta
        ON (ovta.id_mes = dvta.id_mes AND
            ovta.id_pais = dvta.id_pais AND
            ovta.id_provincia = dvta.id_provincia AND
            ovta.id_tienda = dvta.id_tienda AND
            ovta.id_familia = dvta.id_familia AND
            ovta.id_tejido = dvta.id_tejido AND
            ovta.id_modelo = dvta.id_modelo)
      WHEN MATCHED THEN
        UPDATE SET dvta.unidades = ovta.unidades
                  ,dvta.pvp_total = ovta.pvp_total
      WHEN NOT MATCHED THEN
        INSERT 
              (id_mes
              ,id_pais
              ,id_provincia
              ,id_tienda
              ,id_familia
              ,id_tejido
              ,id_modelo
              ,unidades
              ,pvp_total)
          VALUES
              (ovta.id_mes
              ,ovta.id_pais
              ,ovta.id_provincia
              ,ovta.id_tienda
              ,ovta.id_familia
              ,ovta.id_tejido
              ,ovta.id_modelo
              ,ovta.unidades
              ,ovta.pvp_total);
    COMMIT;
    fin_log(id);
  end Cargar_Fact_Ventas;

  procedure Cargar_Agr_Ventas1 is
    id number;
  begin
    id := inicio_log('Cargar_Agr_Ventas');
    merge into destino.agr_ventas_1 agr_ventas
      using 
        (select id_mes
                ,id_tienda
                ,id_familia
                ,sum(unidades)  unidades
                ,sum(pvp_total)  pvp_total
          from destino.fact_ventas
          group by id_mes
                  ,id_tienda
                  ,id_familia
         ) ventas
      on (ventas.id_mes = agr_ventas.id_mes and
          ventas.id_tienda = agr_ventas.id_tienda and
          ventas.id_familia = agr_ventas.id_familia)
    when matched then
      update set agr_ventas.unidades = ventas.unidades
                ,agr_ventas.pvp_total = ventas.pvp_total
    when not matched then
      insert (id_mes
              ,id_tienda
              ,id_familia
              ,unidades
              ,pvp_total)
      values (ventas.id_mes
              ,ventas.id_tienda
              ,ventas.id_familia
              ,ventas.unidades
              ,ventas.pvp_total);
    commit;           
    fin_log(id);
  end Cargar_Agr_Ventas1;  
  
  --Llamada a todos los procedimientos de carga
  PROCEDURE Cargar_Destino_Ventas IS
    id number;
  BEGIN
    id := inicio_log('Cargar_Destino_Ventas');
    Comprueba_Zonas(id);
    Comprueba_Tejidos(id);
    Des_Fk_Destino;
    --Dimensiones
    Cargar_Dim_Meses;
    Cargar_Dim_Paises;
    Cargar_Dim_Provincias;
    Cargar_Dim_Tejidos;
    Cargar_Dim_Familias;
    Cargar_Dim_Modelos;
    Cargar_Dim_Tiendas;
    --Hechos
    Cargar_Fact_Ventas;
    Cargar_Agr_Ventas1;
    Act_Fk_Destino;
    fin_log(id);
  END Cargar_Destino_Ventas;
  
  /*---------------------------------------------------------------------------*\
   | Modelo estrella de precios                                                |
  \*---------------------------------------------------------------------------*/
  procedure Cargar_Fact_Precios is
    type t_array is varray(3) of varchar2(6);
    vTarifa t_array := t_array('venta', 'oferta', 'remate');
    id number;
  begin
    id := inicio_log('Cargar_Fact_Precios');
    for nPai in 1..4 loop
      for nTar in 1..3 loop
        vComando := 
          'merge into destino.fact_precios dpre ' ||
              'using (select m.id_modelo id_modelo, ' || 
                        'm.precio_' || vTarifa(nTar) || '_pais' || 
                        to_char(nPai) || ' precio ' ||
                    'from origen.modelos m) omod ' ||
              'on (dpre.id_modelo = omod.id_modelo and ' ||
                  'dpre.id_pais = ' || to_char(nPai) || ' and ' ||
                  'dpre.id_tarifa = ' || to_char(nTar) || ') ' ||
            'when matched then ' ||
                'update set dpre.precio = omod.precio ' ||
            'when not matched then ' ||
                'insert (dpre.id_modelo, dpre.id_pais, dpre.id_tarifa' ||
                        ', dpre.precio) ' ||
                      'values (omod.id_modelo, ' || to_char(nPai) || ', ' || to_char(nTar) ||
                      ', omod.precio)';
        execute immediate vComando;
      end loop;
    end loop;
    commit;
    fin_log(id);
  end Cargar_Fact_Precios;

  procedure Cargar_Dim_Tarifas is
    id number;
  begin
    id := inicio_log('Cargar_Dim_Tarifas');
    merge into destino.dim_tarifas dtar
      using (select 1 id_tarifa, 'Venta' tarifa from dual
                union all select 2, 'Oferta' from dual
                union all select 3, 'Remate' from dual) ori
           on (dtar.id_tarifa = ori.id_tarifa)
      when matched then
          update set dtar.tarifa = ori.tarifa
      when not matched then
          insert (id_tarifa, tarifa) 
            values (ori.id_tarifa, ori.tarifa);
    commit;
    fin_log(id);
  end Cargar_Dim_Tarifas;
  
  procedure Cargar_Destino_Precios is
    id number;
  begin
    id:=inicio_log('Cargar_Destino_Precios');
    Des_Fk_Destino;
    --Dimensiones
    Cargar_Dim_Tarifas;
    Cargar_Dim_Modelos;
    Cargar_Dim_Paises;
    --Hechos
    Cargar_Fact_Precios;
    Act_Fk_Destino;
    fin_log(id);
  end Cargar_Destino_Precios;
  
  procedure Des_Fk_Destino is
    id number;
  begin
    id := inicio_log('Des_Fk_Destino');
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_familia';
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_meses';
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_modelo';
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_provincias';
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_pais';
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_tejido';
    execute immediate 
      'alter table destino.fact_ventas disable constraint fk_tienda';
    execute immediate 
      'alter table destino.agr_ventas_1 disable constraint fk_agr_meses';
    execute immediate 
      'alter table destino.agr_ventas_1 disable constraint fk_agr_tienda';
    execute immediate 
      'alter table destino.agr_ventas_1 disable constraint fk_agr_familia';
    execute immediate 
      'alter table destino.fact_precios disable constraint fk_modelo_p';
    execute immediate 
      'alter table destino.fact_precios disable constraint fk_pais_p';
    execute immediate 
      'alter table destino.fact_precios disable constraint fk_tarifa_p';
    fin_log(id);
  end Des_Fk_Destino;

  procedure Act_Fk_Destino is
    id number;
  begin
    id := inicio_log('Act_Fk_Destino');
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_familia';
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_meses';
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_modelo';
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_provincias';
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_pais';
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_tejido';
    execute immediate 
      'alter table destino.fact_ventas enable constraint fk_tienda';
    execute immediate 
      'alter table destino.agr_ventas_1 enable constraint fk_agr_meses';
    execute immediate 
      'alter table destino.agr_ventas_1 enable constraint fk_agr_tienda';
    execute immediate 
      'alter table destino.agr_ventas_1 enable constraint fk_agr_familia';
    execute immediate 
      'alter table destino.fact_precios enable constraint fk_modelo_p';
    execute immediate 
      'alter table destino.fact_precios enable constraint fk_pais_p';
    execute immediate 
      'alter table destino.fact_precios enable constraint fk_tarifa_p';
    fin_log(id);
  end Act_Fk_Destino;
  
  --Borrar toda la informacion de las tablas de dimensiones
  procedure Borrar_Dimensiones is
    id number;
  begin
    id := inicio_log('Borrar_Dimensiones');
    Des_Fk_Destino;
    delete destino.dim_tarifas;
    delete destino.dim_modelos;
    delete destino.dim_paises;
    delete destino.dim_meses;
    delete destino.dim_provincias;
    delete destino.dim_tiendas;
    delete destino.dim_familias;
    delete destino.dim_tejidos;
    commit;
    fin_log(id);
  end Borrar_Dimensiones;
  
begin
  -- Initialization
  vComando := NULL;
end pkCarga_ETL;
/
