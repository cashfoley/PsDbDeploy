IF @@TRANCOUNT > 0 COMMIT TRANSACTION;

CREATE FULLTEXT INDEX ON [Production].[Document](
[Document] TYPE COLUMN [FileExtension] LANGUAGE [English], 
[DocumentSummary] LANGUAGE [English])
KEY INDEX [PK_Document_DocumentNode]ON ([AW2008FullTextCatalog], FILEGROUP [PRIMARY])
WITH (CHANGE_TRACKING = AUTO, STOPLIST = SYSTEM)
GO
