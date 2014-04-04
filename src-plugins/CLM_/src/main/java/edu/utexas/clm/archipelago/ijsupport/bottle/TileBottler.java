package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.FijiArchipelago;
import edu.utexas.clm.archipelago.ijsupport.bottle.tile.AffineTile2DBottle;
import edu.utexas.clm.archipelago.ijsupport.bottle.tile.GenericAffineTile2DBottle;
import edu.utexas.clm.archipelago.ijsupport.bottle.tile.RigidTile2DBottle;
import edu.utexas.clm.archipelago.ijsupport.bottle.tile.SimilarityTile2DBottle;
import edu.utexas.clm.archipelago.ijsupport.bottle.tile.TranslationTile2DBottle;
import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.AffineTile2D;
import mpicbg.trakem2.align.GenericAffineTile2D;
import mpicbg.trakem2.align.RigidTile2D;
import mpicbg.trakem2.align.SimilarityTile2D;
import mpicbg.trakem2.align.TranslationTile2D;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 *
 */
public class TileBottler implements Bottler<AbstractAffineTile2D<?>>, Serializable
{
    private boolean isOrigin = true;

    public boolean accepts(Object o)
    {
        return o instanceof AbstractAffineTile2D;
    }

    public Bottle<AbstractAffineTile2D<?>> bottle(Object o, MessageXC xc)
    {
        if (o instanceof AffineTile2D)
        {
            return new AffineTile2DBottle((AffineTile2D)o, isOrigin);
        }
        else if (o instanceof RigidTile2D)
        {
            return new RigidTile2DBottle((RigidTile2D)o, isOrigin);
        }
        else if (o instanceof SimilarityTile2D)
        {
            return new SimilarityTile2DBottle((SimilarityTile2D)o, isOrigin);
        }
        else if (o instanceof TranslationTile2D)
        {
            return new TranslationTile2DBottle((TranslationTile2D)o, isOrigin);
        }
        else if (o instanceof GenericAffineTile2D)
        {
            return new GenericAffineTile2DBottle((GenericAffineTile2D)o, isOrigin);
        }
        else
        {
            FijiArchipelago.log("I don't know how to bottle a " + o.getClass().getName());
            return null;
        }
    }

    public boolean transfer()
    {
        return true;
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException
    {
        ois.defaultReadObject();

        isOrigin = false;
    }
}
