using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.ContentSearch.Utilities;
using Sitecore.ContentSearch.SolrProvider;
using SolrNet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Web;
using SolrNet.Impl;
using Microsoft.Practices.ServiceLocation;

namespace Sitecore.Support.ContentSearch.SolrProvider
{
  public class SwitchOnRebuildSolrSearchIndex : Sitecore.ContentSearch.SolrProvider.SwitchOnRebuildSolrSearchIndex
  {
    private ISolrOperations<Dictionary<string, object>> tempSolrOperations;
    public SwitchOnRebuildSolrSearchIndex(string name, string core, string rebuildcore, IIndexPropertyStore propertyStore) : base(name, core, rebuildcore, propertyStore)
    {
    }
    protected override void PerformRebuild(bool resetIndex, bool optimizeOnComplete, IndexingOptions indexingOptions, CancellationToken cancellationToken)
    {
      if (!this.ShouldStartIndexing(indexingOptions))
        return;


      using (new RebuildIndexingTimer(this.PropertyStore))
      {
        this.tempSolrOperations = ServiceLocator.Current.GetInstance<ISolrOperations<Dictionary<string, object>>>(this.RebuildCore);
        if (resetIndex)
        {
          this.Reset(this.tempSolrOperations, this.RebuildCore);
        }

        using (var context = this.CreateTemporaryCoreUpdateContext(this.tempSolrOperations))
        {
          foreach (var crawler in this.Crawlers)
          {
            crawler.RebuildFromRoot(context, indexingOptions, cancellationToken);
          }

          context.Commit();
        }

        if (optimizeOnComplete)
        {
          CrawlingLog.Log.Debug(string.Format("[Index={0}] Optimizing core [Core: {1}]", this.Name, this.RebuildCore));
          this.tempSolrOperations.Optimize();
        }
      }
      if ((this.IndexingState & IndexingState.Stopped) == IndexingState.Stopped)
      {
        CrawlingLog.Log.Debug(string.Format("[Index={0}] Swapping of cores was not done since full rebuild was stopped...", this.Name));
        return;
      }

      this.SwapCores();
    }
    private void SwapCores()
    {
      CrawlingLog.Log.Debug(string.Format("[Index={0}] Swapping cores [{1} -> {2}]", this.Name, this.Core, this.RebuildCore));
      var solrAdmin = SolrContentSearchManager.SolrAdmin as SolrCoreAdmin;
      var response = solrAdmin.Swap(this.Core, this.RebuildCore);

      if (response.Status != 0)
      {
        CrawlingLog.Log.Error(string.Format("[Index={0}] Error swapping cores. [{1}]", this.Name, this.RebuildCore));
      }
    }    
    private void Reset(ISolrOperations<Dictionary<string, object>> operations, string coreName)
    {
      CrawlingLog.Log.Debug(string.Format("[Index={0}] Resetting index records [Core: {1}]", this.Name, coreName));

      var query = new SolrQueryByField("_indexname", this.Name);
      operations.Delete(query);
      operations.Commit();
    }
  }
}