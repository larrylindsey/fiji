package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.ijsupport.bottle.TileBottle;
import edu.utexas.clm.archipelago.network.MessageXC;
import ini.trakem2.display.Patch;
import mpicbg.models.Model;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.GenericAffineTile2D;

import java.io.IOException;

/**
 *
 */
public class GenericAffineTile2DBottle extends TileBottle
{
    private static class IDGenericAffineTile2D extends GenericAffineTile2D
    {
        private final long id;

        public IDGenericAffineTile2D(final Model<?> model, final Patch patch, long id)
        {
            super(model, patch);
            this.id = id;
        }
    }

    private final Model<?> model;

    public GenericAffineTile2DBottle(final GenericAffineTile2D tile, boolean isOrigin)
    {
        super(isOrigin, tile.getPatch());

        model = tile.getModel();

        if (isOrigin)
        {
            id = getNextId();
            placeTile(id, tile);
        }
        else
        {
            if (tile instanceof IDGenericAffineTile2D)
            {
                id = ((IDGenericAffineTile2D)tile).id;
            }
            else
            {
                id = -1;
            }
        }
    }

    public AbstractAffineTile2D<?> unBottle(final MessageXC xc) throws IOException
    {
        if (fromOrigin)
        {
            return new IDGenericAffineTile2D(model, patch, id);
        }
        else
        {
            if (id > 0)
            {
                return getTile(id);
            }
            else
            {
                return new GenericAffineTile2D(model, patch);
            }
        }
    }
}
