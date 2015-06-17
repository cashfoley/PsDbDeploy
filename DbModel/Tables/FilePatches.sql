CREATE TABLE [PsDbDeploy].[FilePatches](
    [OID] [bigint] IDENTITY(1,1) NOT NULL,
    [FilePath] [nvarchar](450) NOT NULL,
    [Applied] [datetime] NOT NULL,
    [CheckSum] [nvarchar] (100) NOT NULL,
    [Comment] [nvarchar] (4000)
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [UIDX_PsDbDeployFilePatches_FilePath] ON [PsDbDeploy].[FilePatches]
(
    [FilePath] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)