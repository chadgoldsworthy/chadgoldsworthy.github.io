SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER FUNCTION [report].[Fn_ReintermediationIds](@EndDate date)
RETURNS TABLE 
AS
RETURN
(

    WITH Relationships AS (
        SELECT DISTINCT
            p.PolicyNumber,
            b.BrokerCode,
            pb.PolicyId,
            pb.BrokerId,
            bc.Id AS BrokerConsultantId,
            pb.StartDate,
            pb.EndDate,
            bca.Channel,
            LAG(bca.Channel) OVER (PARTITION BY pb.PolicyId ORDER BY pb.StartDate) AS PreviousChannel
        FROM store.rl_dim_all_policy_broker_attributes pb
        JOIN store.slx_dim_all_broker_consultant_broker_relationship bcr
            ON pb.BrokerId = bcr.BrokerId
            AND pb.StartDate BETWEEN bcr.StartDate AND bcr.EndDate
            AND bcr.BackRelation LIKE '%Broker Consultant%'
        LEFT JOIN store.slx_entity_all_broker_consultant bc
            ON bcr.BrokerConsultantId = bc.Id
        LEFT JOIN store.slx_dim_all_broker_consultant_attributes bca
            ON bc.Id = bca.BrokerConsultantId
            AND pb.StartDate BETWEEN bca.StartDate AND bca.EndDate
        JOIN store.entity_all_broker b
            ON pb.BrokerId = b.Id
        JOIN store.entity_all_policy p
            ON pb.PolicyId = p.Id
    ),

    FilteredRelationships AS (
        SELECT * FROM Relationships
        WHERE StartDate BETWEEN DATEADD(day,1,DATEADD(MONTH, -3, @EndDate)) AND @EndDate
        AND PreviousChannel = '<private>'
    ),

    MonthlyFlows AS (
        SELECT 
            @EndDate AS MonthEndDate,
            t.UnitHolderId,
            uha.Id AS UnitHolderAttributesId,
            t.ProductId,
            c.PolicyId,
            pa.Id AS PolicyAttributesId,
            c.BrokerId,
            c.BrokerConsultantId,
            c.Channel,
            c.PreviousChannel,
            pa.PolicyCreationDate,
            c.StartDate AS TransferDate,
            SUM(CASE WHEN t.EffectiveDate < c.StartDate THEN ABS(t.GrossAmount) ELSE 0 END) AS FlowsPriorTransfer,
            SUM(CASE WHEN t.EffectiveDate >= c.StartDate THEN ABS(t.GrossAmount) ELSE 0 END) AS FlowsPostTransfer
        FROM FilteredRelationships c
        JOIN store.rl_fact_unit_holder_month_aggregate_transactions t
            ON c.PolicyId = t.PolicyId
        LEFT JOIN store.rl_dim_all_unit_holder_attributes uha
            ON t.UnitHolderId = uha.UnitHolderId
            AND t.EffectiveDate BETWEEN uha.StartDate AND uha.EndDate
        LEFT JOIN store.rl_dim_all_policy_attributes pa
            ON t.PolicyId = pa.PolicyId
            AND t.EffectiveDate BETWEEN pa.StartDate AND pa.EndDate
        WHERE t.EffectiveDate BETWEEN DATEADD(day,1,DATEADD(MONTH, -12, @EndDate)) AND @EndDate
            AND t.ContributionType IN ('C','W')
            AND pa.PolicyCreationDate >= DATEADD(day,1,DATEADD(MONTH, -6, @EndDate))
            AND c.Channel <> c.PreviousChannel
        GROUP BY    t.UnitHolderId,uha.Id,t.ProductId,c.PolicyId,pa.Id,c.BrokerId,c.BrokerConsultantId,
                    c.Channel,c.PreviousChannel,pa.PolicyCreationDate,c.StartDate
    ),


    AllWithdrawals AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY t.PolicyId ORDER BY t.EffectiveDate ASC) AS row_num,
            MIN(t.EffectiveDate) AS TransactionDate,
            t.PolicyId,
            t.ProductId,
            SUM(t.GrossAmount) AS Amount
        FROM store.rl_fact_unit_holder_month_aggregate_transactions t
        WHERE t.ContributionType = 'W'
        AND t.EffectiveDate BETWEEN DATEADD(day,1,DATEADD(MONTH, -9, @EndDate)) AND @EndDate
        GROUP BY t.EffectiveDate,t.PolicyId,t.ProductId
    ),

    FilteredWithdrawals AS (
        SELECT * FROM AllWithdrawals
        WHERE row_num = 1
    ),

    BaseInflows AS (
        SELECT
            EOMONTH(ag.EffectiveDate) AS EOMDate,
            ag.PolicyId,
            SUM(CASE WHEN ag.[TransactionType] IN ('LS', 'DO') AND ag.[MancoType] = 'MM' THEN ag.GrossAmount ELSE 0 END) AS MMTotalGrossFlow,
            SUM(CASE WHEN ag.[TransactionType] IN ('LS', 'DO') AND ag.[MancoType] = 'TP' THEN ag.GrossAmount ELSE 0 END) AS TPTotalGrossFlow,
            SUM(CASE WHEN ag.[TransactionType] IN ('LS', 'DO') THEN ag.GrossAmount ELSE 0 END) AS TotalGrossFlow
        FROM store.rl_fact_unit_holder_month_aggregate_transactions ag
        WHERE ag.EffectiveDate BETWEEN DATEADD(day,1,DATEADD(MONTH, -12, @EndDate)) AND @EndDate
        AND ag.[TransactionType] != 'X'
        GROUP BY EOMONTH(ag.EffectiveDate), ag.PolicyId
    ),

    TotalInflows AS (
        SELECT
            PolicyId,
            SUM(TotalGrossFlow) AS TotalInflows,
            SUM(MMTotalGrossFlow) AS MMFlows,
            SUM(TPTotalGrossFlow) AS TPFlows,
            SUM(MMTotalGrossFlow) / NULLIF(SUM(TotalGrossFlow),0) AS MMFlowPercentage,
            SUM(TPTotalGrossFlow) / NULLIF(SUM(TotalGrossFlow),0) AS TPFlowPercentage
        FROM BaseInflows
        GROUP BY PolicyId
    ),

    preReintermediation AS (
        SELECT
            mf.MonthEndDate,
            mf.UnitHolderId,
            mf.UnitHolderAttributesId,
            mf.ProductId,
            mf.PolicyId,
            mf.PolicyAttributesId,
            mf.BrokerId,
            mf.BrokerConsultantId,
            mf.Channel,
            mf.PreviousChannel,
            mf.PolicyCreationDate,
            mf.TransferDate,
            w.TransactionDate AS WithdrawalDate,
            mf.FlowsPriorTransfer,
            mf.FlowsPostTransfer,
            ISNULL(w.Amount,0) AS WithdrawalAmount,
            NULLIF(w.Amount,0) / NULLIF(mf.FlowsPriorTransfer,0) AS ProportionWithdrawal_Prior,
            NULLIF(w.Amount,0) / NULLIF(mf.FlowsPostTransfer,0) AS ProportionWithdrawal_Post,
            ISNULL(tif.TotalInflows,0) AS TotalInflows,
            ISNULL(tif.MMFlows,0) AS MMFlows,
            ISNULL(tif.TPFlows,0) AS TPFlows,
            ISNULL(tif.MMFlowPercentage,0) AS MMFlowPercentage,
            ISNULL(tif.TPFlowPercentage,0) AS TPFlowPercentage,
            ISNULL(tif.MMFlowPercentage * mf.FlowsPriorTransfer,0) AS FinalMM,
            ISNULL(tif.TPFlowPercentage * mf.FlowsPriorTransfer,0) AS FinalTP
        FROM MonthlyFlows mf
        LEFT JOIN FilteredWithdrawals w
            ON mf.PolicyId = w.PolicyId
            AND mf.ProductId = w.ProductId
        LEFT JOIN TotalInflows tif
            ON mf.PolicyId = tif.PolicyId
    ),

    InvestmentSpecialists AS (
        SELECT DISTINCT
            bcr.StartDate,
            bcr.EndDate,
            bc.Id AS BrokerConsultantId,
            u.Id AS InvestmentSpecialistId,
            ur.Channel as ChannelSplit
        FROM [store].[entity_all_user] u
        INNER JOIN [store].[dist_dim_all_broker_consultant_user_relationship] ur
            ON ur.UserId = u.Id
        INNER JOIN [store].[slx_entity_all_broker_consultant] bc
            ON bc.Id = ur.BrokerConsultantId
        INNER JOIN [store].[slx_dim_all_broker_consultant_broker_relationship] bcr
            ON bcr.BrokerConsultantId = bc.Id
        JOIN store.entity_all_broker b
            ON b.Id = bcr.BrokerId
        WHERE bcr.BackRelation LIKE '%Broker Consultant%'
            AND u.Role = 'Investment Specialist'
    )

    
    SELECT DISTINCT
        r.*,
        i.InvestmentSpecialistId,
        i.ChannelSplit
    FROM preReintermediation r
    LEFT JOIN InvestmentSpecialists i
        ON r.BrokerConsultantId = i.BrokerConsultantId
        AND r.TransferDate BETWEEN i.StartDate AND i.EndDate
    WHERE (r.FinalMM > 0 OR r.FinalTP > 0)

)

GO
