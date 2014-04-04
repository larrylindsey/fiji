package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.display.Patch;
import mpicbg.trakem2.align.AbstractAffineTile2D;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public abstract class TileBottle implements Bottle<AbstractAffineTile2D<?>>
{
    private static final AtomicLong nextId = new AtomicLong(0);
    private static final Map<Long, AbstractAffineTile2D<?>> idTileMap =
            Collections.synchronizedMap(new HashMap<Long, AbstractAffineTile2D<?>>());

    protected long getNextId()
    {
        return nextId.getAndIncrement();
    }

    protected long id;
    protected final boolean fromOrigin;
    protected final Patch patch;

    public TileBottle(final boolean isOrigin, final Patch patch)
    {
        fromOrigin = isOrigin;
        this.patch = patch;
    }

    protected void placeTile(final long id, final AbstractAffineTile2D<?> tile)
    {
        idTileMap.put(id, tile);
    }

    protected AbstractAffineTile2D<?> getTile(final long id)
    {
        return idTileMap.get(id);
    }
}
