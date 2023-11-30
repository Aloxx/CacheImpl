package et.dontclans.cache;

import et.dontclans.DontClans;
import et.dontclans.config.ConstantMessages;
import et.dontclans.pkg.clan.Clan;
import et.dontclans.pkg.uniqueGenerator.UniqueIdGenerator;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import ru.evelapi.pkg.StringAPI;
import java.util.HashSet;
import static org.bukkit.Bukkit.getLogger;
import static org.bukkit.Bukkit.getScheduler;
public class ClansCacheLayer {
    /**
     * Big Cache , slower than L2 But stores all clans in RAM
     */
    private final Object2ObjectOpenHashMap<String, Clan> layout1Map;
    /**
     * Fast Cache aka L2 Cache , used for faster Callback
     */
    private final Object2ObjectOpenHashMap<String, Clan> layout2Map;

    private HashSet<Clan> removalTransactions;
    /**
     * Cache inst
     * @param cache
     */
    private final DCache cache;

    public ClansCacheLayer (DCache cache) {
        /**
         * Inits obstacle ints
         */
        this.layout1Map = new Object2ObjectOpenHashMap<>();
        this.layout2Map = new Object2ObjectOpenHashMap<>();

        /**
         * Async transactions to make sure if async calls not locks db
         */
        this.removalTransactions = new HashSet<>();

        /**
         * Cache provider inst
         */
        this.cache = cache;
        /**
         * Debug stuff
         */
        if(cache.isDebugEnabled())
            getLogger()
                    .info(ConstantMessages.PREFIX + StringAPI.asColor("Started Loading &aL1 & L2 &fCache"));

        /**
         * Load clans into RAM
         */
        this.loadAllClansToCache(System.currentTimeMillis(), true);
    }
    /**
     * Create clan logic
     */
    public String createEntry (String entryName, String leaderName) {
        String uuid = UniqueIdGenerator.getUniqueId().toString();

        Clan clan = new Clan(uuid, entryName, leaderName, "", 0, "");

        this.persist(clan, true);

        return uuid;
    }
    /**
     * Update clan data logic
     */
    public void persist (Clan entity, boolean hardlyUpdateToDatabase) {
        /**
         * If needs to update in database , we push in db also adds to L1
         */
        if(hardlyUpdateToDatabase) {
            DontClans
                    .getInst()
                    .getClanManager()
                    .persist(entity);
            this.layout1Map
                    .put(entity.getUniqueId(), entity);
        }else {
            /**
             * Just a fast-cache update
             */
            this.layout2Map
                    .put(entity.getUniqueId(), entity);
        }
    }

    /**
     * Delete logic , deletes from cache then adds to transactions
     */
    public void bulkDeleteEntity (Clan entity) {
        this.layout1Map.remove(entity.getUniqueId());
        this.layout2Map.remove(entity.getUniqueId());

        this.removalTransactions.add(entity);
    }
    /**
     * GetByUniqueId Logic ( fastest thing in cache )
     */
    public Clan getEntryByUniqueId (String queryUniqueId) {
        /**
         * Gets entity in fast cache firstly
         */
        Clan entry = this.layout2Map.get(queryUniqueId);

        /**
         * If not found in fast cache we gonna check in slow cache
         */
        if(entry == null) {
            entry = this.layout1Map.get(queryUniqueId);

            /**
             * If found we push to fast cache and returns entry
             */
            if(entry != null)
                this.layout2Map.put(entry.getUniqueId(), entry);
        }

        return entry;
    }
    /**
     * GetByName Logic ( slowest thing in cache )
     * tip: If you have an uuid , better to get by UniqueId , since its keyof o2o
     */
    public Clan getEntryByName (String queryName) {
        /**
         * Gets entity in fast cache firstly
         */
        Clan entry = this.getEntryInFastCache(queryName);

        /**
         * If not found in fast cache we gonna check in slow cache
         */
        if(entry == null) {
            entry = this.getEntryInSlowCache(queryName);

            /**
             * If found we push to fast cache and returns entry
             */
            if(entry != null)
                this.layout2Map.put(entry.getUniqueId(), entry);
        }

        return entry;
    }

    /**
     * FastCache entry check
     * @param queryName - despires name of clan to get
     * @return entry - returns clan if found, null if not found
     */
    public Clan getEntryInFastCache (String queryName) {
        /**
         * Iterate through fast cache
         */
        for(Clan entry : this.layout2Map.values())
            //compare name to
            if(entry.getName().equals(queryName))
                return entry;

        return null;
    }

    /**
     * SlowCache entry check - global analys
     * @param queryName - despires name of clan to get
     * @return entry - returns clan if found, null if not found
     */
    public Clan getEntryInSlowCache (String queryName) {
        /**
         * Iterate through slow cache
         */
        for(Clan entry : this.layout1Map.values())
            //compare name to
            if(entry.getName().equals(queryName))
                return entry;

        return null;
    }

    /**
     * Loads all clans into RAM ( Slow & Fast cache )
     * @param elapsed - timestamp of load start , to despire load time
     * @param async - if true we run task in async mode
     */
    public void loadAllClansToCache (long elapsed , boolean async) {
        /**
         * Just a task
         */
        Runnable task = () -> {
            /**
             * HashSet used since HashSet has O(1) time for .contains , .add
             * instead of any other tree/list/map ( it has O(n) time for .<methodname> )
             */
            HashSet<Clan> clansSet = DontClans
                    .getInst()
                    .getClanManager()
                    .getClanBase()
                    .sqlGetAllClans();

            /**
             * Just loads to pure cache
             */
            for(Clan clan : clansSet) {
                this.layout1Map
                        .put(clan.getUniqueId(), clan);
            }

            /**
             * Log thingy
             */
            if(this.cache.isDebugEnabled())
                getLogger()
                        .info(ConstantMessages.PREFIX + StringAPI.asColor("Finished Loading &aL1 & L2 &fCache in &a" + (System.currentTimeMillis() - elapsed) + "ms, &floaded &a" + clansSet.size() + "&f clans!"));
        };

        /**
         * Completes task
         */
        if(async) getScheduler().runTaskAsynchronously(DontClans.getInst(), task);
        else task.run();
    }
}
