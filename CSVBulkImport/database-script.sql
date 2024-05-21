USE [master]
GO
CREATE DATABASE [CSVBulkImport]
GO
ALTER DATABASE [CSVBulkImport] SET COMPATIBILITY_LEVEL = 130
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
    begin
        EXEC [CSVBulkImport].[dbo].[sp_fulltext_database] @action = 'enable'
    end
GO
ALTER DATABASE [CSVBulkImport] SET ANSI_NULL_DEFAULT OFF
GO
ALTER DATABASE [CSVBulkImport] SET ANSI_NULLS OFF
GO
ALTER DATABASE [CSVBulkImport] SET ANSI_PADDING OFF
GO
ALTER DATABASE [CSVBulkImport] SET ANSI_WARNINGS OFF
GO
ALTER DATABASE [CSVBulkImport] SET ARITHABORT OFF
GO
ALTER DATABASE [CSVBulkImport] SET AUTO_CLOSE OFF
GO
ALTER DATABASE [CSVBulkImport] SET AUTO_SHRINK OFF
GO
ALTER DATABASE [CSVBulkImport] SET AUTO_UPDATE_STATISTICS ON
GO
ALTER DATABASE [CSVBulkImport] SET CURSOR_CLOSE_ON_COMMIT OFF
GO
ALTER DATABASE [CSVBulkImport] SET CURSOR_DEFAULT  GLOBAL
GO
ALTER DATABASE [CSVBulkImport] SET CONCAT_NULL_YIELDS_NULL OFF
GO
ALTER DATABASE [CSVBulkImport] SET NUMERIC_ROUNDABORT OFF
GO
ALTER DATABASE [CSVBulkImport] SET QUOTED_IDENTIFIER OFF
GO
ALTER DATABASE [CSVBulkImport] SET RECURSIVE_TRIGGERS OFF
GO
ALTER DATABASE [CSVBulkImport] SET  DISABLE_BROKER
GO
ALTER DATABASE [CSVBulkImport] SET AUTO_UPDATE_STATISTICS_ASYNC OFF
GO
ALTER DATABASE [CSVBulkImport] SET DATE_CORRELATION_OPTIMIZATION OFF
GO
ALTER DATABASE [CSVBulkImport] SET TRUSTWORTHY OFF
GO
ALTER DATABASE [CSVBulkImport] SET ALLOW_SNAPSHOT_ISOLATION OFF
GO
ALTER DATABASE [CSVBulkImport] SET PARAMETERIZATION SIMPLE
GO
ALTER DATABASE [CSVBulkImport] SET READ_COMMITTED_SNAPSHOT OFF
GO
ALTER DATABASE [CSVBulkImport] SET HONOR_BROKER_PRIORITY OFF
GO
ALTER DATABASE [CSVBulkImport] SET RECOVERY SIMPLE
GO
ALTER DATABASE [CSVBulkImport] SET  MULTI_USER
GO
ALTER DATABASE [CSVBulkImport] SET PAGE_VERIFY CHECKSUM
GO
ALTER DATABASE [CSVBulkImport] SET DB_CHAINING OFF
GO
ALTER DATABASE [CSVBulkImport] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF )
GO
ALTER DATABASE [CSVBulkImport] SET TARGET_RECOVERY_TIME = 60 SECONDS
GO
ALTER DATABASE [CSVBulkImport] SET DELAYED_DURABILITY = DISABLED
GO
ALTER DATABASE [CSVBulkImport] SET QUERY_STORE = OFF
GO
USE [CSVBulkImport]
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
USE [CSVBulkImport]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Data](
                             [id] [int] IDENTITY(1,1) NOT NULL,
                             [tpep_pickup_datetime] [datetimeoffset](7) NOT NULL,
                             [tpep_dropoff_datetime] [datetimeoffset](7) NOT NULL,
                             [passenger_count] [int] NULL,
                             [trip_distance] [float] NOT NULL,
                             [store_and_fwd_flag] [nvarchar](3) NOT NULL,
                             [PULocationID] [int] NULL,
                             [DOLocationID] [int] NULL,
                             [fare_amount] [float] NULL,
                             [tip_amount] [float] NULL,
                             CONSTRAINT [PK_Data] PRIMARY KEY CLUSTERED
                                 (
                                  [id] ASC
                                     )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[SelectDuplicatesView]
AS
SELECT        id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, store_and_fwd_flag, PULocationID, DOLocationID, fare_amount, tip_amount
FROM            dbo.Data
WHERE        (id NOT IN
              (SELECT        MIN(id) AS Expr1
               FROM            dbo.Data AS Data_1
               GROUP BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count))
GO
CREATE NONCLUSTERED INDEX [PULocationID_tip_amount-index] ON [dbo].[Data]
    (
     [PULocationID] ASC,
     [tip_amount] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [PULocationId-index] ON [dbo].[Data]
    (
     [PULocationID] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [terms_of_time-index] ON [dbo].[Data]
    (
     [tpep_pickup_datetime] ASC,
     [tpep_dropoff_datetime] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [trip_distance-index] ON [dbo].[Data]
    (
     [trip_distance] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[ConvertDatesToUTC]
AS
BEGIN
    UPDATE [dbo].[Data]
    SET
        [tpep_pickup_datetime] = DATEADD(hh, DATEDIFF(hh, CONVERT(DATETIME,GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'), GETUTCDATE()), [tpep_pickup_datetime]),
        [tpep_dropoff_datetime] = DATEADD(hh, DATEDIFF(hh, CONVERT(DATETIME,GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'), GETUTCDATE()), [tpep_dropoff_datetime])
END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[DeleteDuplicatedRecords]
AS
BEGIN
    DELETE FROM dbo.Data
    WHERE
        id NOT IN (
            SELECT MIN(id)
            FROM dbo.Data
            GROUP BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
        )
END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[UpdateFlags]
AS
BEGIN
    UPDATE dbo.Data
    SET store_and_fwd_flag = 'YES'
    WHERE store_and_fwd_flag = 'Y'

    UPDATE dbo.Data
    SET store_and_fwd_flag = 'NO'
    WHERE store_and_fwd_flag = 'N' OR store_and_fwd_flag IS NULL
END
GO
USE [master]
GO
ALTER DATABASE [CSVBulkImport] SET  READ_WRITE
GO
