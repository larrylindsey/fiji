package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.ijsupport.bottle.TileBottle;
import edu.utexas.clm.archipelago.network.MessageXC;
import ini.trakem2.display.Patch;
import mpicbg.models.AffineModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.AffineTile2D;

import java.io.IOException;

/**
 *
 */
public class AffineTile2DBottle extends TileBottle
{
    private static class IDAffineTile2D extends AffineTile2D
    {
        private final long id;

        public IDAffineTile2D(final AffineModel2D model, final Patch patch, long id)
        {
            super(model, patch);
            this.id = id;
        }
    }

    private final AffineModel2D model;

    public AffineTile2DBottle(final AffineTile2D tile, boolean isOrigin)
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
            if (tile instanceof IDAffineTile2D)
            {
                id = ((IDAffineTile2D)tile).id;
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
            return new IDAffineTile2D(model, patch, id);
        }
        else
        {
            if (id > 0)
            {
                return getTile(id);
            }
            else
            {
                return new AffineTile2D(model, patch);
            }
        }
    }
}
