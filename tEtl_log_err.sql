-- Create table
create table DESTINO.ETL_LOG_ERR
(
  ID        NUMBER(8) not null,
  TEXTO_ERR VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table DESTINO.ETL_LOG_ERR
  add constraint FK_ETL_LOG_ERR foreign key (ID)
  references DESTINO.ETL_LOG (ID);
