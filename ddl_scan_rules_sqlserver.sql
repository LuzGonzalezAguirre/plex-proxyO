-- ============================================================
-- DDL: Scan Rules en SQL Server CCS (AAS-PAC-FTP01 / DATABASE=CCS)
-- Ejecutar UNA VEZ en SQL Server antes de usar los endpoints del proxy.
-- ============================================================

-- 1. Tabla principal de reglas
IF OBJECT_ID(N'[dbo].[quality_pn_scan_rules]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[quality_pn_scan_rules] (
        [id]             INT          IDENTITY(1,1) NOT NULL,
        [pn_id]          INT          NOT NULL,
        [ssi_pn]         NVARCHAR(20) NOT NULL,
        [bu_id]          INT          NOT NULL,
        [bu_name]        NVARCHAR(50) NOT NULL,
        [scan_count]     INT          NOT NULL CONSTRAINT [DF_pnsr_scan_count]     DEFAULT 1,
        [requires_match] BIT          NOT NULL CONSTRAINT [DF_pnsr_req_match]      DEFAULT 0,
        [notes]          NVARCHAR(MAX) NOT NULL CONSTRAINT [DF_pnsr_notes]         DEFAULT N'',
        [is_active]      BIT          NOT NULL CONSTRAINT [DF_pnsr_is_active]      DEFAULT 1,
        [created_by_id]  INT          NULL,
        [created_at]     DATETIME2    NOT NULL CONSTRAINT [DF_pnsr_created_at]     DEFAULT GETUTCDATE(),
        [updated_by_id]  INT          NULL,
        [updated_at]     DATETIME2    NOT NULL CONSTRAINT [DF_pnsr_updated_at]     DEFAULT GETUTCDATE(),
        CONSTRAINT [PK_quality_pn_scan_rules] PRIMARY KEY CLUSTERED ([id] ASC),
        CONSTRAINT [UQ_quality_pn_scan_rules_pn_id] UNIQUE ([pn_id])
    );
    PRINT 'Tabla quality_pn_scan_rules creada.';
END
ELSE
    PRINT 'Tabla quality_pn_scan_rules ya existe — omitida.';
GO

-- 2. Tabla de campos de escaneo
IF OBJECT_ID(N'[dbo].[quality_scan_fields]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[quality_scan_fields] (
        [id]               INT           IDENTITY(1,1) NOT NULL,
        [rule_id]          INT           NOT NULL,
        [scan_index]       INT           NOT NULL,
        [extraction_mode]  NVARCHAR(20)  NOT NULL CONSTRAINT [DF_qsf_extraction_mode]  DEFAULT N'completo',
        [field_target]     NVARCHAR(30)  NOT NULL,
        [separator]        NVARCHAR(20)  NOT NULL CONSTRAINT [DF_qsf_separator]        DEFAULT N'ninguno',
        [separator_custom] NVARCHAR(10)  NOT NULL CONSTRAINT [DF_qsf_separator_custom] DEFAULT N'',
        [value_position]   NVARCHAR(10)  NOT NULL CONSTRAINT [DF_qsf_value_position]   DEFAULT N'completo',
        [segment_index]    INT           NULL,
        [fixed_length]     INT           NULL,
        [prefix_value]     NVARCHAR(50)  NOT NULL CONSTRAINT [DF_qsf_prefix_value]     DEFAULT N'',
        [display_label]    NVARCHAR(100) NOT NULL,
        [sequence_order]   INT           NOT NULL CONSTRAINT [DF_qsf_sequence_order]   DEFAULT 0,
        CONSTRAINT [PK_quality_scan_fields] PRIMARY KEY CLUSTERED ([id] ASC),
        CONSTRAINT [FK_quality_scan_fields_rule_id]
            FOREIGN KEY ([rule_id])
            REFERENCES [dbo].[quality_pn_scan_rules] ([id])
            ON DELETE CASCADE
    );

    CREATE NONCLUSTERED INDEX [IX_quality_scan_fields_rule_id]
        ON [dbo].[quality_scan_fields] ([rule_id] ASC);

    PRINT 'Tabla quality_scan_fields creada.';
END
ELSE
    PRINT 'Tabla quality_scan_fields ya existe — omitida.';
GO

-- ============================================================
-- Verificación rápida post-creación
-- ============================================================
SELECT
    t.name        AS tabla,
    c.name        AS columna,
    tp.name       AS tipo,
    c.max_length,
    c.is_nullable,
    c.is_identity
FROM sys.tables    t
JOIN sys.columns   c  ON c.object_id  = t.object_id
JOIN sys.types     tp ON tp.user_type_id = c.user_type_id
WHERE t.name IN ('quality_pn_scan_rules', 'quality_scan_fields')
ORDER BY t.name, c.column_id;
