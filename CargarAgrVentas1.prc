create or replace procedure CargarAgrVentas1 is
begin
  merge into destino.agr_ventas_1 agr_ventas
    using 
      (select id_mes
              ,id_tienda
              ,id_familia
              ,unidades
              ,pvp_total
        from destino.fact_ventas) ventas
    on (ventas.id_mes = agr_ventas.id_mes and
        ventas.id_tienda = agr_ventas.id_tienda and
        ventas.id_familia = agr_ventas.id_familia)
  when matched then
    update set <field> = <value>
  when not matched then
    insert (<list fields>)
    values (<values>)    
    
end CargarAgrVentas1;
/
