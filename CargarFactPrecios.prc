create or replace procedure CargarFactPrecios is
  type t_array is varray(4) of varchar2(6);
  vTarifa t_array := t_array('venta', 'oferta', 'remate');
  vComando varchar2(500);              
begin
    for nPai in 1..4 loop
      for nTar in 1..3 loop
        vComando := 
          'merge into destino.fact_precios dpre ' ||
              'using (select m.id_modelo id_modelo, ' || 
              'm.precio_' || vTarifa(nTar) || '_pais' || to_char(nPai) || ' precio ' ||
              'from origen.modelos m) omod ' ||
              'on (dpre.id_modelo = omod.id_modelo and ' ||
              'dpre.id_pais = omod.id_pais and ' ||
              'dpre.id_tarifa = omod.id_tarifa) ' ||
            'when matched then ' ||
                'update set dpre.precio = omod.precio ' ||
            'when not matched then ' ||
                'insert (dpre.id_modelo, ' || to_char(nPai) || 
                        ', ' || to_char(nTar) || ', dpre.precio) ' ||
                      'values (omod.id_modelo, omod.id_pais, omod.id_tarifa, omod.precio)';
        execute immediate vComando;
        --DBMS_OUTPUT.put_line(vComando);
      end loop;
    end loop;
  execute immediate 'commit';
end CargarFactPrecios;
/
