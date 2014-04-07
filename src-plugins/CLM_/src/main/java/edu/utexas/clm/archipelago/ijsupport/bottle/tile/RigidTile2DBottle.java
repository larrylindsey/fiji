package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.ijsupport.bottle.TileBottle;
import edu.utexas.clm.archipelago.network.MessageXC;
import ini.trakem2.display.Patch;
import mpicbg.models.RigidModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.RigidTile2D;

import java.io.IOException;

/**
 *
 */
public class RigidTile2DBottle extends TileBottle
{
    private static class IDRigidTile2D extends RigidTile2D
    {
        private final long id;

        public IDRigidTile2D(final RigidModel2D model, final Patch patch, long id)
        {
            super(model, patch);
            this.id = id;
        }
    }
    
    private final RigidModel2D model;

    public RigidTile2DBottle(final RigidTile2D tile, final boolean isOrigin)
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
            if (tile instanceof IDRigidTile2D)
            {
                id = ((IDRigidTile2D)tile).id;
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
            synchronized( sync )
            {
                if (hasTile(id))
                {
                    return getTile(id);
                }
                else
                {
                    AbstractAffineTile2D<?> tile = new IDRigidTile2D(model, patch, id);
                    placeTile(id, tile);
                    return tile;
                }
            }
        }
        else
        {
            if (id > 0)
            {
                return getTile(id);
            }
            else
            {
                return new RigidTile2D(model, patch);
            }
        }
    }
}
